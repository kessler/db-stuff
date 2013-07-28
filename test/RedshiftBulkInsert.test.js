var _ = require('underscore');
var RedshiftBulkInsert = require('../lib/RedshiftBulkInsert.js');
var assert = require('assert');

describe('RedshiftBulkInsert', function() {

	it('contructor', function() {

		var options = {
			datastore: 'testDatastore',
			tableName: 'testTable',
			paramsOrFields: 'testParamsOrFields',
			fields: ['xxx', 'yyy'],
			awsBucketName: 'testbucketName',
			pathToLogs: '/dev',
			awsAccessKeyId: 'testAwsAccessKeyId',
			awsSecretAccessKey: 'testAwsSecretAccessKey'
		};

		var mock = {

			_getLogFileName: function() {
				return 'null';
			},

			_createS3: function() {

			}
		};

		RedshiftBulkInsert.bind(mock)(options);

		assert(mock._datastore === options.datastore);
		assert(mock._tableName === options.tableName);
		//assert(mock._paramsOrFields === options.paramsOrFields);
		assert(mock._fields === options.fields);
		assert(mock._awsBucketName === options.awsBucketName);
		assert(mock._pathToLogs === options.pathToLogs);
		assert(mock._awsAccessKeyId === options.awsAccessKeyId);
		assert(mock._awsSecretAccessKey === options.awsSecretAccessKey);

	});

	it('_rowToLine', function () {

		var mock = {

			_escapeValue: RedshiftBulkInsert.prototype._escapeValue

		};

		var row = ['xxx', 123, 0.5, null, undefined];

		var result = RedshiftBulkInsert.prototype._rowToLine.bind(mock)(row);

		assert(result === 'xxx|123|0.5|\\N|\\N');

	});

	describe('_escapeValue', function() {

		it ('null => \\N', function() {

			var result = RedshiftBulkInsert.prototype._escapeValue(null);
			assert(result === '\\N');

		});

		it ('\\ => \\\\', function() {

			var result = RedshiftBulkInsert.prototype._escapeValue('\\');
			assert(result === '\\\\');

		});

		it ('| => \\|', function() {

			var result = RedshiftBulkInsert.prototype._escapeValue('|');
			assert(result === '\\|');

		});

	});

	it('insert', function() {

		var rowToLineCalled = false;
		var fileWrite = false;

		var testRow = 'testRow';

		var mock = {

			_rowToLine: function(row) {
				assert(testRow === row);
				rowToLineCalled = true;
				return row;
			},

			_file: {

				write: function(line) {
					assert(line === testRow + '\n');
					fileWrite = true;
				}

			}
		};

		RedshiftBulkInsert.prototype.insert.bind(mock)(testRow);

		assert(rowToLineCalled);
		assert(fileWrite);

	});

	describe.skip('_onSendedCopyToRedshift', function() {
	});

	it('_getCopyQuery', function() {

		var fileName = '34565467567.log';

		var mock = {
			_fields: ['xxx', 'yyy'],
			_tableName: 'testTable',
			_awsBucketName: 'testBucket',
			_awsAccessKeyId: 'testAwsAccessKeyId',
			_awsSecretAccessKey: 'testAwsSecretAccessKey'
		};

		var expected = "COPY testTable (xxx, yyy) FROM 's3://testBucket/34565467567.log' CREDENTIALS 'aws_access_key_id=testAwsAccessKeyId;aws_secret_access_key=testAwsSecretAccessKey'";

		var result = RedshiftBulkInsert.prototype._getCopyQuery.bind(mock)(fileName);

		assert(result === expected);

	});

	describe('_onSentToS3', function() {

		it('do nothing when pass error', function() {

			var fileName = 'testFileName.log';
			var err = 'Test error';

			RedshiftBulkInsert.prototype._onSentToS3(fileName, err);

		});

		it('do things', function() {

			var testFileName = 'testFileName.log';
			var testCopyQuery = 'testCopyQuery';

			var onSentCopyToRedshiftCalled = false;
			var datastoreQueryCalled = false;
			var getCopyQueryCalled = false;

			var mock = {

				_onSentCopyToRedshift: function() {
					onSentCopyToRedshiftCalled = true;
				},

				_datastore: {

					query: function(sql, callback) {
						assert(testCopyQuery === sql);
						datastoreQueryCalled = true;
						callback();
					}

				},

				_getCopyQuery: function(fileName) {
					assert(testFileName === fileName);
					getCopyQueryCalled = true;
					return testCopyQuery;
				}
			};

			RedshiftBulkInsert.prototype._onSentToS3.bind(mock)(testFileName);

			assert(onSentCopyToRedshiftCalled);
			assert(datastoreQueryCalled);
			assert(getCopyQueryCalled);

		});

	});

	describe('_sendToS3', function() {

		it('do nothing when pass error', function() {

			var testFileName = 'testFileName.log';
			var testErr = 'test error';

			RedshiftBulkInsert.prototype._sendToS3(testFileName, testErr);

		});

		it('do things', function() {

			var testFileName = 'testFileName.log';
			var testAwsBucketName = 'testAwsBucketName';
			var testErr = null;
			var testBody = 'testBody';

			var expectedParams = {
				Body: testBody,
				Key: testFileName,
				Bucket: testAwsBucketName
			};

			var onSentToS3Called = false;
			var s3bucketPutObject = false;

			var mock = {

				_awsBucketName: testAwsBucketName,

				_onSentToS3: function(fileName) {
					assert(fileName === testFileName);
					onSentToS3Called = true;
				},

				_s3: {

					putObject: function(params, callback) {
						assert.deepEqual(params, expectedParams);
						s3bucketPutObject = true;
						callback();
					}
				}
			};

			RedshiftBulkInsert.prototype._sendToS3.bind(mock)(testFileName, testErr, testBody);

			assert(onSentToS3Called);
			assert(s3bucketPutObject);

		});

	});

	it('flush rotates and reads file', function() {

		var testFileName = 'null';
		var testPathToLogs = '/dev';

		var fileSetupFileCalled = false;
		var sendToS3Called = false;
		var startIdleFlushMonitor = false;

		var mock = {

			_fileName: testFileName,
			_pathToLogs: testPathToLogs,
			_hasEventsInFile: true,

			_getLogFileName: function() {
				return 'newFileName.log';
			},

			startIdleFlushMonitor: function() {
				startIdleFlushMonitor = true;
			},

			_file: {

				setupFile: function(fileName) {
					assert(_.isString(fileName));
					assert(fileName !== testFileName);
					fileSetupFileCalled = true;
				}

			},

			_sendToS3: function(fileName) {
				assert(fileName === testFileName);
				sendToS3Called = true;
			}

		}

		RedshiftBulkInsert.prototype.flush.bind(mock)();

		assert(mock._fileName !== testFileName);

		assert(fileSetupFileCalled);
		//assert(sendToS3Called);
		assert(startIdleFlushMonitor);

	});

});