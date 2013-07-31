var async = require('async');
var uuid = require('node-uuid');
var path = require('path');
var aws = require('aws-sdk');
var EventEmitter = require('events').EventEmitter;
var $u = require('util');
var fs = require('fs');
var SimpleFileWriter = require('simple-file-writer');

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

	this._fileWriter = new SimpleFileWriter();

	this._datastore = datastore;
	
	this._inserts = 0;

	this.activeFlushOps = 0;
	
	this._resetFilenameState();
	
	this._rotateFilename();
}

RedshiftBulkInsert.prototype.insert = function(row, insertDoneCallback) {
	var self = this;
	var parts = 0;

	function insertPart(part, callback) {
		part = self._escapeValue(part);

		if (++parts < row.length)
			part += DELIMITER;

		self._fileWriter.write(new Buffer(part, 'utf8'), callback);
	}
	
	function writeNewLine(err) {
		
		if (err) {
			if (insertDoneCallback)
				insertDoneCallback(err);
			else
				throw err;
		}

		self._fileWriter.write(NEWLINE, insertDone);		
	}

	function insertDone() {
		
		self._inserts++;
		
		if (self._inserts === self._threshold) {

			self._stopIdleFlushMonitor();

			var flushOp = self.flush();
		
			if (insertDoneCallback)
				insertDoneCallback(null, flushOp);		

		} else if (insertDoneCallback) {
			insertDoneCallback(null);		
		}

		self._startIdleFlushMonitor();
	}

	async.each(row, insertPart, writeNewLine);	
};

RedshiftBulkInsert.prototype.flush = function () {

	if (this._inserts === 0) return;

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

RedshiftBulkInsert.prototype._readFile = function(flushOp) {
	return function(callback) {
		
		flushOp.stage = '_readFile';

		fs.readFile(flushOp.filename, callback);
	};
};

RedshiftBulkInsert.prototype._uploadFileToS3 = function (flushOp) {
	var self = this;

	return function(body, callback) {		
		
		flushOp.stage = '_uploadFileToS3';

		self._s3.putObject({
			Body: body,
			Key: flushOp.filename
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

RedshiftBulkInsert.prototype._rotateFilename = function () {
	this._fileWriter.setupFile(this._generateFilename());
};

RedshiftBulkInsert.prototype._resetFilenameState = function () {
	this._flushCounter = 0;
	this._uniqueness = uuid();
	this._prefix = this._tableName + '-' + process.pid + '-' + this._uniqueness + '-';
};

RedshiftBulkInsert.prototype._generateFilename = function () {
	
	if (this._flushCounter === MAX) 
		this._resetFilenameState();

	this._filename = path.join(this._path, this._prefix + this._flushCounter + this._extension);

	return this._filename;
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
	this._start = Date.now();
	this._bulkInsert.activeFlushOps++;

	this._bulkInsert._rotateFilename();

	async.waterfall([

		this._bulkInsert._readFile(this),
		this._bulkInsert._uploadFileToS3(this),
		this._bulkInsert._executeCopyQuery(this)

	], this.done());
	
};

FlushOperation.prototype.done = function () {

	var self = this;
	return function (err, results) {
		self._bulkInsert.activeFlushOps--;
		self._bulkInsert._inserts = 0;

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


