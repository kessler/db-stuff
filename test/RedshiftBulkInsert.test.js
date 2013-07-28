var _ = require('underscore');
var BulkInsert2 = require('../lib/BulkInsert2.js');
var assert = require('assert');

describe('BulkInsert2', function() {

	it.skip('contructor', function() {

		var datastore = 'testDatastore';
		var table = 'testTable';
		var paramsOrFields = 'testParamsOrFields';
		var fields = ['xxx', 'yyy'];
		var bucketName = 'testbucketName';
		var pathToLogs = 'testPathToLogs';
		var awsAccessKeyId = 'testAwsAccessKeyId';
		var awsSecretAccessKey = 'testAwsSecretAccessKey';

		var result = new BulkInsert2(datastore, table, paramsOrFields,
			fields, bucketName, pathToLogs, awsAccessKeyId, awsSecretAccessKey);

		assert(this._datastore === datastore);
		assert(this._table === table);
		assert(this._paramsOrFields === paramsOrFields);
		assert(this._fields === fields);
		assert(this._bucketName === bucketName);
		assert(this._pathToLogs === pathToLogs);
		assert(this._awsAccessKeyId === awsAccessKeyId);
		assert(this._awsSecretAccessKey === awsSecretAccessKey);

	});

	it('_rowToLine', function () {

		var mock = {

			_escapeValue: BulkInsert2.prototype._escapeValue

		};

		var row = ['xxx', 123, 0.5, null, undefined];

		var result = BulkInsert2.prototype._rowToLine.bind(mock)(row);

		assert(result === 'xxx|123|0.5|\\N|\\N');

	});

	describe('_escapeValue', function() {

		it ('null => \\N', function() {

			var result = BulkInsert2.prototype._escapeValue(null);
			assert(result === '\\N');

		});

		it ('\\ => \\\\', function() {

			var result = BulkInsert2.prototype._escapeValue('\\');
			assert(result === '\\\\');

		});

		it ('| => \\|', function() {

			var result = BulkInsert2.prototype._escapeValue('|');
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

		BulkInsert2.prototype.insert.bind(mock)(testRow);

		assert(rowToLineCalled);
		assert(fileWrite);

	});

	describe.skip('_onSendedCopyToRedshift', function() {
	});

	it('_getCopyQuery', function() {

		var fileName = '34565467567.log';

		var mock = {
			_tableName: 'testTable',
			_bucketName: 'testBucket',
			_awsAccessKeyId: 'testAwsAccessKeyId',
			_awsSecretAccessKey: 'testAwsSecretAccessKey'
		};

		var expected = "COPY testTable FROM 's3://testBucket/34565467567.log'"
			+ " CREDENTIALS 'aws_access_key_id=testAwsAccessKeyId;aws_secret_access_key=testAwsSecretAccessKey'"
			+ "  DELIMITER '|' NULL AS '\\N' ESCAPE '\\'";

		var result = BulkInsert2.prototype._getCopyQuery.bind(mock)(fileName);

		assert(result === expected);

	});

	describe('_onSentToS3', function() {

		it('do nothing when pass error', function() {

			var fileName = 'testFileName.log';
			var err = 'Test error';

			BulkInsert2.prototype._onSentToS3(fileName, err);

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

			BulkInsert2.prototype._onSentToS3.bind(mock)(testFileName);

			assert(onSentCopyToRedshiftCalled);
			assert(datastoreQueryCalled);
			assert(getCopyQueryCalled);

		});

	});

	describe('_sendToS3', function() {

		it('do nothing when pass error', function() {

			var testFileName = 'testFileName.log';
			var testErr = 'test error';

			BulkInsert2.prototype._sendToS3(testFileName, testErr);

		});

		it('do things', function() {

			var testFileName = 'testFileName.log';
			var testErr = null;
			var testBody = 'testBody';

			var expectedParams = {
				Body: testBody,
				Key: testFileName
			};

			var onSentToS3Called = false;
			var s3bucketPutObject = false;

			var mock = {

				_onSentToS3: function(fileName) {
					assert(fileName === testFileName);
					onSentToS3Called = true;
				},

				_s3bucket: {

					putObject: function(params, callback) {
						assert.deepEqual(params, expectedParams);
						s3bucketPutObject = true;
						callback();
					}
				}
			};

			BulkInsert2.prototype._sendToS3.bind(mock)(testFileName, testErr, testBody);

			assert(onSentToS3Called);
			assert(s3bucketPutObject);

		});

	});

	it('flush rotates and reads file', function() {

		var testFileName = 'testFileName.log';
		var testPathToLogs = 'testPathToLogs.log';

		var fileSetupFileCalled = false;
		var sendToS3Called = false;

		var mock = {

			_fileName: testFileName,
			_pathToLogs: testPathToLogs,

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

		BulkInsert2.prototype.flush.bind(mock)();

		assert(mock._fileName !== testFileName);

		assert(fileSetupFileCalled);
		//assert(sendToS3Called);

	});

});