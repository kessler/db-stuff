var _ = require('underscore');
var RedshiftBulkInsert = require('../lib/RedshiftBulkInsert.js');
var assert = require('assert');

describe('RedshiftBulkInsert', function() {

	it('contructor', function() {

		var startIdleFlushMonitorCalled = false;

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

			},

			startIdleFlushMonitor: function() {
				startIdleFlushMonitorCalled = true;
			}
		};

		RedshiftBulkInsert.bind(mock)(options);

		assert(startIdleFlushMonitorCalled);

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

		it ('shit', function() {
			var testValue = 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; iOpus-I-M; GTB6.6; 001|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)';
			var result = RedshiftBulkInsert.prototype._escapeValue(testValue);
			assert(result === 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; iOpus-I-M; GTB6.6; 001\\|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)');

			var testValue = 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; 001|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; (R1 1.6); .NET CLR 1.1.4322; .NET CLR 2.0.50727; SRS_IT_E8790272B376545A36AD92)';
			var result = RedshiftBulkInsert.prototype._escapeValue(testValue);
			assert(result === 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; 001\\|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; (R1 1.6); .NET CLR 1.1.4322; .NET CLR 2.0.50727; SRS_IT_E8790272B376545A36AD92)');

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

		it('calls _decrimentActiveFlushOpsAndEmitFlushEvent when error is passed', function() {

			var fileName = 'testFileName.log';
			var err = 'Test error';
			var testStart = 678966745645;

			var decrimentActiveFlushOpsAndEmitFlushEventCalled = false;

			var mock = {

				_decrimentActiveFlushOpsAndEmitFlushEvent: function(start, err, result) {
					assert(start === testStart);
					assert(err === err);
					decrimentActiveFlushOpsAndEmitFlushEventCalled = true;
				}

			};

			RedshiftBulkInsert.prototype._onSentToS3.bind(mock)(fileName, testStart, err);

			assert(decrimentActiveFlushOpsAndEmitFlushEventCalled);

		});

		it('do things when error is not passed', function() {

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

		it('calls _decrimentActiveFlushOpsAndEmitFlushEvent when error is passed', function() {

			var testFileName = 'testFileName.log';
			var testErr = 'test error';
			var testStart = 4353457;

			var decrimentActiveFlushOpsAndEmitFlushEventCalled = false;

			var mock = {

				_decrimentActiveFlushOpsAndEmitFlushEvent: function(start, err, result) {
					assert(testStart === start);
					assert(err === testErr);
					decrimentActiveFlushOpsAndEmitFlushEventCalled = true;
				}

			};

			RedshiftBulkInsert.prototype._sendToS3.bind(mock)(testFileName, testStart, testErr);

			assert(decrimentActiveFlushOpsAndEmitFlushEventCalled);

		});

		it('do things', function() {

			var testFileName = 'testFileName.log';
			var testStart = 56455767;
			var testAwsBucketName = 'testAwsBucketName';
			var testErr = null;
			var testBody = 'testBody';

			var expectedParams = {
				Body: testBody,
				Key: testFileName,
				Bucket: testAwsBucketName
			};

			var onSentToS3Called = false;
			var decrimentActiveFlushOpsAndEmitFlushEventNotCalled = true;
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

			RedshiftBulkInsert.prototype._sendToS3.bind(mock)(testFileName, testStart, testErr, testBody);

			assert(onSentToS3Called);
			assert(s3bucketPutObject);

		});

	});

	describe('flush', function() {

		it('do nothing if number of events is equals to zero', function() {

			var startIdleFlushMonitorCalled = false;

			var mock = {

				_numberOfEventsInFile: 0,
				activeFlushOps: 0,

				startIdleFlushMonitor: function() {
					startIdleFlushMonitorCalled = true;
				}
			};

			RedshiftBulkInsert.prototype.flush.bind(mock)();

			assert(mock.activeFlushOps === 0);
			assert(startIdleFlushMonitorCalled);

		});

		it('rotates when number of events is not equals to zero', function() {

			var testFileName = 'null';
			var testPathToLogs = '/dev';
			var testNewFileName = 'newFileName.log';

			var fileSetupFileCalled = false;
			var sendToS3Called = false;
			var startIdleFlushMonitor = false;
			var fsReadFileCalled = false;

			var mock = {

				_fileName: testFileName,
				_pathToLogs: testPathToLogs,
				_hasEventsInFile: true,
				_numberOfEventsInFile: 5,
				activeFlushOps: 2,

				_getLogFileName: function() {
					return testNewFileName;
				},

				startIdleFlushMonitor: function() {
					startIdleFlushMonitor = true;
				},

				_file: {

					setupFile: function(fileName) {
						assert(_.isString(fileName));
						assert(mock._fileName === testNewFileName);
						assert(fileName !== testFileName);
						fileSetupFileCalled = true;
					}

				},

				_fs: {
					readFile: function(path, callback) {
						assert(path === '/dev/null');
						callback();
						fsReadFileCalled = true;
					}
				},

				_sendToS3: function(fileName) {
					assert(fileName === testFileName);
					sendToS3Called = true;
				}

			}

			RedshiftBulkInsert.prototype.flush.bind(mock)();

			assert(mock.activeFlushOps === 3);
			assert(mock._fileName === testNewFileName);
			assert(mock._fileName !== testFileName);
			assert(mock.activeFlushOps);

			assert(fileSetupFileCalled);
			assert(sendToS3Called);
			assert(startIdleFlushMonitor);
			assert(fsReadFileCalled);

		});

	});

	it('_decrimentActiveFlushOpsAndEmitFlushEvent', function() {

		var testError = 'testError';
		var testStart = 46576767;
		var testResult = 'testResult';

		var emitCalled = false;

		var mock = {

			activeFlushOps: 5,

			emit: function(type, err, result, sql, start, bulkInsert) {
				assert(type === 'flush');
				assert(err === testError);
				assert(result === testResult);
				assert(start === testStart);
				assert(bulkInsert === mock);
				emitCalled = true;
			}
		};

		RedshiftBulkInsert.prototype._decrimentActiveFlushOpsAndEmitFlushEvent.bind(mock)(testStart, testError, testResult);

		assert(mock.activeFlushOps === 4);
		assert(emitCalled);

	});

});