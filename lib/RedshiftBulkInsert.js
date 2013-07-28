var aws = require('aws-sdk');
var _ = require('underscore');
var util = require('util');
var SimpleFileWriter = require('simple-file-writer');
var log4js = require('log4js');
var path = require('path');
var fs = require('fs');
var events = require('events');

var EventEmitter = events.EventEmitter;

var logger = log4js.getLogger('redshift-bulk-insert');

module.exports = RedshiftBulkInsert;

var DELIMITER = '|';
var ESCAPE = '\\';
var NEWLINE = '\n';
var NULL = '\\N';
var SUFFIX = '.log';

util.inherits(RedshiftBulkInsert, EventEmitter);

function RedshiftBulkInsert(options) {

	var pathToLogs = options.pathToLogs;

	this._datastore = options.datastore;
	this._tableName = options.tableName;
	this._fields = options.fields;
	this._pathToLogs = pathToLogs;

	var idleFlushPeriod = options.idleFlushPeriod;
	this._idleFlushPeriod = _.isNumber(idleFlushPeriod) ? idleFlushPeriod : 10000;

	var threshold = options.threshold;
	this._threshold = _.isNumber(threshold) ? threshold : 1000;

	this._awsBucketName = options.awsBucketName;
	this._awsAccessKeyId = options.awsAccessKeyId;
	this._awsSecretAccessKey = options.awsSecretAccessKey;
	this._awsrRegion = options.awsRegion;

	this._numberOfEventsInFile = 0;
	this._activeFlushOps = 0;

	var fileName = this._getLogFileName();
	var pathToFile = path.join(pathToLogs, fileName);

	this._fileName = fileName;
	this._file = new SimpleFileWriter(pathToFile);
	this._s3 = this._createS3();

	this.startIdleFlushMonitor();
}

RedshiftBulkInsert.prototype._getLogFileName = function() {
	return this._tableName + '_' + Date.now() + SUFFIX;
}

RedshiftBulkInsert.prototype._createS3 = function() {
	var options = {
		region: this._awsRegion,
		accessKeyId: this._awsAccessKeyId,
		secretAccessKey: this._awsSecretAccessKey
	};
	return new aws.S3(options);
}

RedshiftBulkInsert.prototype.flush = function() {

	logger.info('flush');

	if (this._numberOfEventsInFile > 0) {

		this.activeFlushOps++;

		var oldFileName = this._fileName;
		var pathToLogs = this._pathToLogs;

		var newFileName = this._getLogFileName();
		this._fileName = newFileName;
		var newPathToFile = path.join(pathToLogs, newFileName);
		this._file.setupFile(newPathToFile);

		var sendToS3 = _.bind(this._sendToS3, this, oldFileName, Date.now());
		var oldPathToFile = path.join(pathToLogs, oldFileName);
		fs.readFile(oldPathToFile, sendToS3);

		this._numberOfEventsInFile = 0;
	}

	this.startIdleFlushMonitor();
};

RedshiftBulkInsert.prototype._sendToS3 = function(fileName, start, err, body) {
	if (err) {
		logger.error(err);
		return;
	}
	var params = {
		Body: body,
		Key: fileName,
		Bucket: this._awsBucketName
	};
	var onSentToS3 = _.bind(this._onSentToS3, this, fileName, start);
	this._s3.putObject(params, onSentToS3);
};

RedshiftBulkInsert.prototype._onSentToS3 = function(fileName, start, err, data) {
	if (err) {
		logger.error(err);
		return;
	}
	var onSentCopyToRedshift = _.bind(this._onSentCopyToRedshift, this, fileName, start);
	var query = this._getCopyQuery(fileName);
	this._datastore.query(query, onSentCopyToRedshift);
};

RedshiftBulkInsert.prototype._getCopyQuery = function(fileName) {
	return 'COPY '
		+ this._tableName
		+ ' ('
		+ this._fields.join(', ')
		+ ')'
		+ ' FROM '
		+ "'"
		+ 's3://'
		+ this._awsBucketName
		+ '/'
		+ fileName
		+ "'"
		+ ' CREDENTIALS '
		+ "'aws_access_key_id="
		+ this._awsAccessKeyId
		+ ';'
		+ 'aws_secret_access_key='
		+ this._awsSecretAccessKey
		+ "' ";
		// + ' DELIMITER '
		// + "'"
		// + DELIMITER
		// + "'"
		// + ' NULL AS '
		// + "'"
		// + NULL
		// + "'"
		// + ' ESCAPE '
		// + "'"
		// + ESCAPE
		// + "'";
};

RedshiftBulkInsert.prototype._onSentCopyToRedshift = function(fileName, start, err, result) {
	if (err) {
		logger.error(err);
	}

	var pathToFile = path.join(this._pathToLogs, fileName);
	fs.unlink(pathToFile, _.bind(this._onLogFileRemoved, this, err, result));
};

RedshiftBulkInsert.prototype._onLogFileRemoved = function(start, err, result) {
	this.activeFlushOps--;
	this.emit('flush', err, result, '', Date.now() - start, this);
};

RedshiftBulkInsert.prototype.insert = function(row) {

	var line = this._rowToLine(row);
	this._file.write(line + NEWLINE);
	this._numberOfEventsInFile++;

	if (this._numberOfEventsInFile > this._threshold) {
		clearTimeout(this.ref);
		this.flush();
		return;
	}
};

RedshiftBulkInsert.prototype._rowToLine = function(row) {
	var lineBuff = [];
	for (var i = 0, l = row.length; i < l; i++) {
		var value = row[i];
		value = this._escapeValue(value);
		lineBuff.push(value);
	}
	var line = lineBuff.join(DELIMITER);
	return line;
};

RedshiftBulkInsert.prototype._escapeValue = function(value) {
	if (value === null || value === undefined) {
		return NULL;
	}
	if (_.isString(value)) {
		return value
			.replace(ESCAPE, ESCAPE + ESCAPE)
			.replace(DELIMITER, ESCAPE + DELIMITER);
	}
	return value + '';
};

RedshiftBulkInsert.prototype.close = function() {
	clearTimeout(this.ref);
};

RedshiftBulkInsert.prototype.startIdleFlushMonitor = function() {
	logger.info('startIdleFlushMonitor');

	var flush = _.bind(this.flush, this);
	this.ref = setTimeout(flush, this._idleFlushPeriod);
};