var async = require('async');
var uuid = require('node-uuid');
var path = require('path');
var aws = require('aws-sdk');
var EventEmitter = require('events').EventEmitter;
var $u = require('util');

var NULL = '\\N';
var DELIMITER = '|';
var NEWLINE = new Buffer('\n', 'utf8');
var EXTENSION = '.log';

RedshiftBulkInsert.FlushOperation = FlushOperation;
module.exports = RedshiftBulkInsert;

var MAX = Math.pow(2, 53);

/*
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

	this._path = options.path || __dirname;

	this._max = options.max || MAX;

	this._extension = options.extension || EXTENSION;

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

	if (!$u.isArray(options.fields))
		throw new Error('missing fields in options');

	if (options.fields.length === 0)
		throw new Error('missing fields in options');

	this._fields = [].concat(options.fields);

	this._s3 = this._createS3Client(awsOptions);
	
	this._awsOptions = awsOptions;

	this._datastore = datastore;
	
	this._currentBufferLength = 0;

	this._buffer = [];

	this.activeFlushOps = 0;	
}

RedshiftBulkInsert.prototype.insert = function(row) {
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

	return flushOp; // ok that this is undefined when no flush occurs
};

RedshiftBulkInsert.prototype.flush = function () {

	if (this._buffer.length === 0) return;

	var flushOp = this._newFlushOperation();
	
	flushOp.start();	

	return flushOp;
};

RedshiftBulkInsert.prototype._newFlushOperation = function () {
	return new FlushOperation(this, this._filename);
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
	
	return function(callback) {		

		var copyQuery = self._generateCopyQuery(flushOp.filename);
		
		flushOp.stage = '_executeCopyQuery';
		flushOp.copyQuery = copyQuery;
		self._datastore.query(copyQuery, callback);
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

function FlushOperation(bulkInsert, filename) {	
	this._bulkInsert = bulkInsert;
	this.filename = filename;
}

FlushOperation.prototype.start = function () {
	this.buffer = this._bulkInsert._buffer;
	this.bufferLength = this._bulkInsert._currentBufferLength;

	this._bulkInsert._buffer = [];
	this._bulkInsert._currentBufferLength = 0;
	this._bulkInsert.activeFlushOps++;	

	this._start = Date.now();

	async.waterfall([

		//this._bulkInsert._readFile(this),
		this._bulkInsert._uploadToS3(this),
		this._bulkInsert._executeCopyQuery(this),
		//this._bulkInsert._deleteLocalFile(this),

	], this.done());
	
};

FlushOperation.prototype.done = function () {

	var self = this;
	return function (err, results) {
		self._bulkInsert.activeFlushOps--;
		
		if (err) {
			if (self.stage = '_executeCopyQuery') {
				self._bulkInsert.emit('flush', err, results, self.copyQuery, self._start, self._bulkInsert);
			} else {
				self._bulkInsert.emit('flush', err, null, self.stage, self._start, self._bulkInsert);
			}
		}

		self._bulkInsert.emit('flush', null, results, self.copyQuery, self._start, self._bulkInsert);	
	};	
};


