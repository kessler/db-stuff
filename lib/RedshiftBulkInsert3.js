var async = require('async');
var uuid = require('node-uuid');
var path = require('path');
var aws = require('aws-sdk');
var EventEmitter = require('events').EventEmitter;
var $u = require('util');
var ip = require('ip');

var NULL = '\\N';
var DELIMITER = '|';
var NEWLINE = new Buffer('\n', 'utf8');
var EXTENSION = '.log';

RedshiftBulkInsert.FlushOperation = FlushOperation;
module.exports = RedshiftBulkInsert;

var MAX = Math.pow(2, 53);
var ipAddress = ip.address();
var pid = process.pid;

/*
	@param options - {
		fields: 			[an array of the table fields involved in the copy],
		delimiter: 			[delimiter to use when writing the copy files],
		tableName: 			[target table],
		extension: 			[the extension of the files written to s3],
		threshold: 			[the number of events that trigger a flush],
		idleFlushPeriod: 	[longtest time events will stay in the buffer before flush (if threshold is not met then this will be the time limit for flushing)],
		autoVacuum: 		[a boolean indicating if a vacuum operation should be executed after each insert]
	}

	@param awsOptions - { region: ..., accessKeyId: ..., secretAccessKey: ..., bucket: ...}

*/
$u.inherits(RedshiftBulkInsert, EventEmitter);
function RedshiftBulkInsert(datastore, options, awsOptions) {
	EventEmitter.call(this);

	if (options === undefined)
		throw new Error('missing options');

	if (options.delimiter === undefined)
		this.delimiter = DELIMITER;
	else
		this.delimiter = options.delimiter;

	if (options.tableName === undefined)
		throw new TypeError('missing table name');

	this._tableName = options.tableName;

	this._extension = options.extension || EXTENSION;

	if (!$u.isArray(options.fields))
		throw new Error('missing fields in options');

	if (options.fields.length === 0)
		throw new Error('missing fields in options');

	this._fields = [].concat(options.fields);

	if (options.threshold === 0)
		throw new Error('cannot set threhold to 0');

	if (options.threshold === undefined)
		options.threshold = 1000;

	this._threshold = options.threshold;

	if (options.idleFlushPeriod === 0)
		throw new Error('cannot set idleFlushPeriod to 0');

	if (options.idleFlushPeriod === undefined)
		options.idleFlushPeriod = 5000;

	this._idleFlushPeriod = options.idleFlushPeriod;

	this._s3 = this._createS3Client(awsOptions);

	this._awsOptions = awsOptions;

	this._datastore = datastore;

	this._currentBufferLength = 0;

	this._buffer = [];

	this.activeFlushOps = 0;
}

RedshiftBulkInsert.prototype.insert = function(row) {
	if (row === undefined) return;
	if (row.length === 0) return;

	var self = this;
	var parts = 0;

	var text = '';
	for (var i = 0; i < row.length; i++) {
		if (i > 0)
			text += this.delimiter;

		text += this._escapeValue(row[i]);
	}

	var rowBuffer = new Buffer(text + NEWLINE, 'utf8');

	this._buffer.push(rowBuffer);
	this._currentBufferLength += rowBuffer.length;

	var flushOp;

	if (this._buffer.length === this._threshold) {

		this._stopIdleFlushMonitor();

		flushOp = this.flush();
	}

	this._startIdleFlushMonitor();

	return flushOp; // its ok that this is undefined when no flush occurs
};

RedshiftBulkInsert.prototype.flush = function () {

	if (this._buffer.length === 0) return;

	var buffer = this._buffer;
	var bufferLength = this._currentBufferLength;
	this._buffer = [];
	this._currentBufferLength = 0;
	this.activeFlushOps++;

	var flushOp = this._newFlushOperation(buffer, bufferLength);

	flushOp.start(this);

	return flushOp;
};

RedshiftBulkInsert.prototype._newFlushOperation = function (buffer, bufferLength) {
	return new FlushOperation(buffer, bufferLength, this._generateFilename());
};

RedshiftBulkInsert.prototype._startIdleFlushMonitor = function () {
	var self = this;

	// do not start if we're already started
	if (self._timeoutRef) return;

	self._timeoutRef = setTimeout(function() {
		self._timeoutRef = undefined;
		self.flush();

	}, self._idleFlushPeriod);

	self._timeoutRef.unref();
};

RedshiftBulkInsert.prototype._stopIdleFlushMonitor = function () {
	clearTimeout(this._timeoutRef);
	this._timeoutRef = undefined;
};

RedshiftBulkInsert.prototype._uploadToS3 = function (flushOp) {
	var self = this;

	return function(callback) {
		flushOp.uploadStart = Date.now();
		flushOp.stage = '_uploadToS3';

		self._s3.putObject({
			Body: Buffer.concat(flushOp.buffer, flushOp.bufferLength),
			Key: flushOp.filename,
			Bucket: self._awsOptions.bucket
		}, callback);
	};
};

RedshiftBulkInsert.prototype._executeCopyQuery = function (flushOp) {
	var self = this;

	return function(data, callback) {
		flushOp.queryStart = Date.now();
		var copyQuery = self._generateCopyQuery(flushOp.filename);

		flushOp.stage = '_executeCopyQuery';
		flushOp.copyQuery = copyQuery;
		self._datastore.query(copyQuery, callback);
	};
};

RedshiftBulkInsert.prototype._deleteFileOnS3 = function(flushOp) {
	var self = this;

	return function(queryResults, callback) {
		flushOp.stage = '_deleteFileOnS3';

		self._s3.deleteObject({
			Key: flushOp.filename,
			Bucket: self._awsOptions.bucket
		}, function(err, data) {
			callback(queryResults, data);
		});
	};
};

RedshiftBulkInsert.prototype._generateCopyQuery = function(filename) {
	return 'COPY '
		+ this._tableName
		+ ' ('
		+ this._fields.join(', ')
		+ ')'
		+ ' FROM '
		+ "'"
		+ 's3://'
		+ this._awsOptions.bucket
		+ '/'
		+ filename
		+ "'"
		+ ' CREDENTIALS '
		+ "'aws_access_key_id="
		+ this._awsOptions.accessKeyId
		+ ';'
		+ 'aws_secret_access_key='
		+ this._awsOptions.secretAccessKey
		+ "'"
		+ ' ESCAPE';
};

RedshiftBulkInsert.prototype._createS3Client = function(options) {
	if (options === undefined)
		throw new Error('missing aws options');

	if (options.accessKeyId === undefined)
		throw new Error('missing aws accessKeyId')

	if (options.bucket === undefined)
		throw new Error('missing aws bucket');

	if (options.secretAccessKey === undefined)
		throw new Error('missing aws secretAccessKey');

	if (options.region === undefined)
		throw new Error('missing aws region');

	return new aws.S3(options);
};

RedshiftBulkInsert.prototype._escapeValue = function(value) {
	if (value === null || value === undefined) {
		return NULL;
	}

	if (typeof(value) === 'string') {
		return value.replace(/\\/g, '\\\\').replace(/\|/g, '\\|');
	}

	return value;
};

RedshiftBulkInsert.prototype._generateFilename = function () {
	return this._tableName + '-' + ipAddress + '-' + pid + '-' + Date.now() + '-' + uuid() + '.' + this._extension;
};

function FlushOperation(buffer, bufferLength, filename) {

	this.filename = filename;
	this.buffer = buffer;
	this.bufferLength = bufferLength;
}

FlushOperation.prototype.start = function (bulkInsert) {

	this.flushStart = Date.now();

	this.uploadLatency = 0;
	this.queryLatency = 0;

	async.waterfall([

		bulkInsert._uploadToS3(this),
		this._updateUploadLatency(),
		bulkInsert._executeCopyQuery(this),
		this._updateQueryLatency()
		/*bulkInsert._deleteFileOnS3(this) */

	], this.done(bulkInsert));

	var self = this;
};

FlushOperation.prototype._updateUploadLatency = function() {
	var self = this;
	return function(uploadData, callback) {
		self.uploadLatency = Date.now() - self.uploadStart;
		callback(null, uploadData);
	};
};

FlushOperation.prototype._updateQueryLatency = function() {
	var self = this;
	return function(queryResults, callback) {
		self.queryLatency = Date.now() - self.queryStart;
		callback(null, queryResults);
	};
};

FlushOperation.prototype.done = function (bulkInsert) {

	var self = this;
	return function (err, results) {

		//TODO the signature of this event is terrible, I cannot change this though because of dependant code
		//at some point I would like to emit the old events only if they have listeners (if its possible at all).

		self.queryResults = results;
		self.activeFlushOps = --bulkInsert.activeFlushOps;

		if (err) {
			if (self.stage === '_executeCopyQuery') {
				bulkInsert.emit('flush', err, results, self.copyQuery, self.flushStart, bulkInsert, self.stage);
			} else {
				bulkInsert.emit('flush', err, null, self.copyQuery, self.flushStart, bulkInsert, self.stage);
			}

			bulkInsert.emit('flush error', err, self);
		}

		bulkInsert.emit('flush', null, results, self.copyQuery, self.flushStart, bulkInsert, self.stage);

		bulkInsert.emit('flush done', self);
	};
};


