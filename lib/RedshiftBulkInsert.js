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

	this._datastore = options.datastore;
	this._tableName = options.tableName;
	this._bucketName = options.bucketName;
	this._fields = options.fields;
	this._pathToLogs = options.pathToLogs;
	this._awsAccessKeyId = options.awsAccessKeyId;
	this._awsSecretAccessKey = options.awsSecretAccessKey;
	this._awsrRegion = options.awsRegion;

	this._fileName = this._getLogFileName();
	var pathToFile = path.join(pathToLogs, this._fileName);
	this._file = new SimpleFileWriter(pathToFile);
	this._s3 = this._createS3();
	this._hasEventsInFile = false;
	this._activeFlushOps = 0;

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

	if (this._hasEventsInFile) {

		this.activeFlushOps++;

		var oldFileName = this._fileName;

		var newFileName = this._getLogFileName();
		this._fileName = newFileName;
		var newPathToFile = path.join(this._pathToLogs, newFileName);
		this._file.setupFile(newPathToFile);

		var sendToS3 = _.bind(this._sendToS3, this, oldFileName);
		var oldPathToFile = path.join(this._pathToLogs, oldFileName);
		fs.readFile(oldPathToFile, sendToS3);
		this._hasEventsInFile = false;
	}

	this.startIdleFlushMonitor();
};

RedshiftBulkInsert.prototype._sendToS3 = function(fileName, err, body) {
	if (err) {
		logger.error(err);
		return;
	}
	var params = {
		Body: body,
		Key: fileName,
		Bucket: this._bucketName
	};
	var onSentToS3 = _.bind(this._onSentToS3, this, fileName);
	this._s3.putObject(params, onSentToS3);
};

RedshiftBulkInsert.prototype._onSentToS3 = function(fileName, err, data) {
	if (err) {
		logger.error(err);
		return;
	}
	var onSentCopyToRedshift = _.bind(this._onSentCopyToRedshift, this, fileName);
	var query = this._getCopyQuery(fileName);
	this._datastore.query(query, onSentCopyToRedshift);
};

RedshiftBulkInsert.prototype._getCopyQuery = function(fileName) {
	return 'COPY '
		+ this._tableName
		+ ' ('
		+ this._fields.join(', ')
		+ ') '
		+ ' FROM '
		+ "'"
		+ 's3://'
		+ this._bucketName
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

RedshiftBulkInsert.prototype._onSentCopyToRedshift = function(fileName, err, result) {
	if (err) {
		logger.error(err);
	}
	this.emit('flush', err, result, '', Date.now(), this);
	var pathToFile = path.join(this._pathToLogs, fileName);
	fs.unlink(pathToFile, function() {
		_activeFlushOps--;
	});
};

RedshiftBulkInsert.prototype.insert = function(row) {
	var line = this._rowToLine(row);
	this._file.write(line + NEWLINE);
	this._hasEventsInFile = true;
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
	var flush = _.bind(this.flush, this);
	this.ref = setTimeout(flush, this.params.idleFlushPeriod);
};