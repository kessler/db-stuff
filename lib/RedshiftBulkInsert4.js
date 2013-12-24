//TODO: now that the state moved into FlushOperation many of the flow logic can be moved there as well.
//the bulk insert's retry behaviour should be configurable
var assert = require('assert');
var async = require('async');
var uuid = require('node-uuid');
var path = require('path');
var knox = require('knox');
var EventEmitter = require('events').EventEmitter;
var $u = require('util');
var ip = require('ip');
var domain = require('domain');

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
function RedshiftBulkInsert(datastore, s3ClientProvider, options, awsOptions) {
	EventEmitter.call(this);
	if (typeof(datastore) !== 'object')
		throw new Error('missing datastore');

	if (typeof(s3ClientProvider) !== 'object')
		throw new Error('missing s3 client provider');

	if (typeof(options) !== 'object')
		throw new Error('missing options');

	if (options.delimiter === undefined)
		this.delimiter = DELIMITER;
	else
		this.delimiter = options.delimiter;

	if (typeof(options.tableName) !== 'string')
		throw new Error('missing or invalid table name');

	this._tableName = options.tableName;

	this._extension = options.extension || EXTENSION;

	if (!$u.isArray(options.fields))
		throw new Error('missing fields');

	if (options.fields.length === 0)
		throw new Error('missing fields');

	this._fields = [].concat(options.fields);

	if (options.threshold === 0)
		throw new Error('cannot set threshold to 0');

	if (options.threshold === undefined)
		options.threshold = 1000;

	this._threshold = options.threshold;

	if (options.idleFlushPeriod === 0)
		throw new Error('cannot set idleFlushPeriod to 0');

	if (options.idleFlushPeriod === undefined)
		options.idleFlushPeriod = 5000;

	this._idleFlushPeriod = options.idleFlushPeriod;

	this._awsOptions = awsOptions;

	if (this._awsOptions === undefined)
		throw new Error('missing aws options');

	if (this._awsOptions.accessKeyId === undefined)
		throw new Error('missing aws accessKeyId')

	if (this._bucket === undefined)
		throw new Error('missing aws bucket');

	if (this._awsOptions.secretAccessKey === undefined)
		throw new Error('missing aws secretAccessKey');

	var bucketParts = this._awsOptions.bucket.split('/');

	this._bucket = bucketParts.shift();

	this._keyPrefix = bucketParts.join('/');

	this._datastore = datastore;

	this._s3ClientProvider = s3ClientProvider;

	this._currentBufferLength = 0;

	this._buffer = [];

	this.activeFlushOps = 0;

	this._ipAddress = ipAddress;

	this._pid = pid;
}

RedshiftBulkInsert.prototype.insert = function(row) {
	if (row === undefined) return;
	if (row.length === 0) return;

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

		this.emit('flush', flushOp);
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

	var filename = this._generateFilename();

	var flushOp = this._newFlushOperation(buffer, bufferLength, this._generateKey(filename), this._generateCopyQuery(filename));

	var self = this;

	flushOp.countDecreasedOnce = false;

	flushOp.on('error', onFlushEvent);
	flushOp.on('done', onFlushEvent);

	function onFlushEvent() {

		// prevent logic errors if code changes in the future
		assert(!flushOp.countDecreasedOnce);
		flushOp.countDecreasedOnce = true;
		self.activeFlushOps--;
	}

	flushOp.start(this._s3ClientProvider.get(this._bucket), this._datastore);

	return flushOp;
};

RedshiftBulkInsert.prototype._newFlushOperation = function (buffer, bufferLength, key, copyQuery) {
	return new FlushOperation(buffer, bufferLength, key, copyQuery);
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

RedshiftBulkInsert.prototype._generateCopyQuery = function(filename) {
	return 'COPY '
		+ this._tableName
		+ ' ('
		+ this._fields.join(', ')
		+ ')'
		+ ' FROM '
		+ "'"
		+ 's3://'
		+ this._bucket
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

RedshiftBulkInsert.prototype._generateKey = function(filename) {
	return this._keyPrefix + '/' + filename;
};

RedshiftBulkInsert.prototype._createS3Client = function() {
	if (this._awsOptions === undefined)
		throw new Error('missing aws options');

	if (this._awsOptions.accessKeyId === undefined)
		throw new Error('missing aws accessKeyId')

	if (this._bucket === undefined)
		throw new Error('missing aws bucket');

	if (this._awsOptions.secretAccessKey === undefined)
		throw new Error('missing aws secretAccessKey');

	if (this._awsOptions.region === undefined)
		throw new Error('missing aws region');

	return knox.createClient({
		key: this._awsOptions.accessKeyId,
		secret: this._awsOptions.secretAccessKey,
		bucket: this._bucket,
		region: this._awsOptions.region
	});
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
	return this._keyPrefix + '/' + this._tableName + '-' + this._ipAddress + '-' + this._pid + '-' + this._now() + '-' + this._uuid() + '.' + this._extension;
};

RedshiftBulkInsert.prototype._uuid = uuid;
RedshiftBulkInsert.prototype._now = Date.now;

$u.inherits(FlushOperation, EventEmitter);
function FlushOperation(buffer, bufferLength, key, copyQuery) {
	EventEmitter.call(this);

	this._keyPrefix = keyPrefix;
	this.copyQuery = copyQuery;
	this.key = key;
	this.buffer = buffer;
	this.bufferLength = bufferLength;
}

FlushOperation.prototype.start = function (s3Client, datastore) {

	//TODO add checks for AbstractS3Client and DatastoreBase
	if (arguments.length < 2)
		throw new Error('missing arguments');

	this.flushStart = Date.now();

	this.uploadLatency = 0;
	this.queryLatency = 0;

	async.waterfall([

		this._uploadToS3(s3Client),
		this._updateUploadLatency(),
		this._executeCopyQuery(datastore),
		this._updateQueryLatency()

	], this.done());
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

FlushOperation.prototype._uploadToS3 = function (s3Client) {
	var self = this;

	return function(callback) {

		self.uploadStart = Date.now();
		self.stage = '_uploadToS3';

		var buffer = Buffer.concat(self.buffer, self.bufferLength);

		function putCallback(err, res) {
			if (err) {
				callback(err);
				return;
			}

			if (res.statusCode !== 200) {
				callback('Response status code should be equals to 200 but was ' + res.statusCode);
				return;
			}

			callback(null, res);
		}

		s3Client.put(buffer, self.key, putCallback);
	};
};

FlushOperation.prototype._executeCopyQuery = function (datastore) {
	var self = this;

	return function(data, callback) {
		self.queryStart = Date.now();
		self.stage = '_executeCopyQuery';
		datastore.query(self.copyQuery, callback);
	};
};

FlushOperation.prototype._deleteFileOnS3 = function(flushOp) {
	var self = this;

	return function(queryResults, callback) {
		self.stage = '_deleteFileOnS3';

		// self._s3.deleteObject({
		// 	Key: self.filename,
		// 	Bucket: self._awsOptions.bucket
		// }, function(err, data) {
		// 	callback(queryResults, data);
		// });
	};
};

FlushOperation.prototype.done = function () {

	var self = this;
	return function (err, results) {

		self.queryResults = results;

		if (err) {
			self.emit('error', err, self);
		} else {
			self.emit('done', self);
		}
	};
};
