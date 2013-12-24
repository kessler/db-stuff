var RedshiftBulkInsert = require('../lib/RedshiftBulkInsert4.js');
var assert = require('assert');
var fs = require('fs');
var path = require('path');
var $u = require('util');
var _u = require('underscore');
var PolyMock = require('polymock');
var dbStuff = require('../index.js');
var EventEmitter = require('events').EventEmitter;

var testroot = path.join(__dirname, 'testfiles');
var logfile = path.join(testroot, 'testlog');

var testRow = [1,2,3,'root', null];
var options = {
	path: testroot,
	tableName: 'liga',
	threshold: 3,
	idleFlushPeriod: 100000,
	fields: ['a', 'b']
};

var awsOptions = {
	region: 'us-standard',
	accessKeyId: '2',
	secretAccessKey: '3',
	bucket: 'asd'
};

var EXPECTED_FILENAME = 'lala.log';
var EXPECTED_KEY = 'test/' + EXPECTED_FILENAME;
var EXPECTED_COPY_QUERY = 'copy 123';

function createMock(threshold) {
	var mock = PolyMock.create();

	mock.dummy = new EventEmitter();
	mock.dummy.start = function () {};
	mock.createMethod('_escapeValue', undefined, { dynamicValue: function(val) { return val.toString(); }});
	mock.createMethod('_startIdleFlushMonitor');
	mock.createMethod('_stopIdleFlushMonitor');
	mock.createMethod('flush', mock.dummy);
	mock.createMethod('_generateFilename', EXPECTED_FILENAME);
	mock.createMethod('_generateCopyQuery', EXPECTED_COPY_QUERY);
	mock.createMethod('_generateKey', EXPECTED_KEY);
	mock.createMethod('_newFlushOperation', mock.dummy);
	mock.createMethod('_uuid', '123');
	mock.createMethod('_now', 'now');

	mock.createProperty('_fields', [ 'a', 'b' ]);
	mock.createProperty('_awsOptions', { accessKeyId: '1', secretAccessKey: '2' });
	mock.createProperty('_buffer', []);
	mock.createProperty('_currentBufferLength', 0);
	mock.createProperty('activeFlushOps', 0);
	mock.createProperty('delimiter', '|');
	mock.createProperty('_threshold', threshold);
	mock.createProperty('_keyPrefix', 'prefix');
	mock.createProperty('_tableName', 'tablename');
	mock.createProperty('_extension', 'log');
	mock.createProperty('_pid', '1');
	mock.createProperty('_ipAddress', '1.1.1.1');
	mock.createProperty('_datastore', {});
	mock.createProperty('_s3ClientProvider', { get: function() {
		return {};
	}});

	return mock;
}

function createDatastoreMock() {
	var mock = PolyMock.create();

	mock.createMethod('query', undefined, { callbackArgs: [null, []] });

	return mock;
}

function createS3ClientProviderMock() {
	var mock = PolyMock.create();

	mock.createMethod('get', {});

	return mock;
}

describe('RedshiftBulkInsert', function() {

	describe('constructor', function () {

		it('throws an error if no datastore is specified', function () {
			try {
				var rbl = new RedshiftBulkInsert();
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing datastore');
			}
		});

		it('throws an error if no s3 client provider is specified', function () {
			try {
				var rbl = new RedshiftBulkInsert(createDatastoreMock());
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing s3 client provider');
			}
		});

		it('throws an error if no options are specified', function () {
			try {
				var rbl = new RedshiftBulkInsert(createDatastoreMock(), createS3ClientProviderMock());
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing options');
			}
		});

		it('throws an error if no table name is not supplied in the options', function () {
			try {
				var rbl = new RedshiftBulkInsert(createDatastoreMock(), createS3ClientProviderMock(),  {});
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing or invalid table name');
			}
		});

		it('throws an error if fields are missing in options', function () {
			try {
				var rbl = new RedshiftBulkInsert(createDatastoreMock(), createS3ClientProviderMock(), { tableName: '123' });
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing fields');
			}
		});

		it('throws an error if fields are missing in options', function () {
			try {
				var rbl = new RedshiftBulkInsert(createDatastoreMock(), createS3ClientProviderMock(), { tableName: '123', fields: [] });
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing fields');
			}
		});

		it('throws an error if threshold is set to 0', function () {
				try {
				var options = { idleFlushPeriod: 0, tableName: 'asdlj', fields: [ '1' ], threshold: 0 };
				var rbl = new RedshiftBulkInsert(createDatastoreMock(), createS3ClientProviderMock(), options);
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'cannot set threshold to 0');
			}
		});

		it('throws an error if idleFlushPeriod is set to 0', function () {
			try {
				var options = { idleFlushPeriod: 0, tableName: 'asdlj', fields: [ '1' ], threshold: 10 };
				var rbl = new RedshiftBulkInsert(createDatastoreMock(), createS3ClientProviderMock(), options);
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'cannot set idleFlushPeriod to 0');
			}
		});

		//TODO complete constructor / initial state tests
	});

	it('generates filenames used in the copy process', function () {
		var mock = createMock(1);

		var filename = RedshiftBulkInsert.prototype._generateFilename.call(mock.object);
		assert.strictEqual(filename, 'prefix/tablename-1.1.1.1-1-now-123.log');
	});

	it('generates a copy query', function () {
		var mock = createMock(1);

		var query = RedshiftBulkInsert.prototype._generateCopyQuery.call(mock.object, '1.log');

		assert.strictEqual(query, 'COPY tablename (a, b) FROM \'s3://undefined/1.log\' CREDENTIALS \'aws_access_key_id=1;aws_secret_access_key=2\' ESCAPE');
	});

	describe('insert', function () {

		it('saves a row in a buffer', function() {
			var buff = new Buffer('1|2|3\n', 'utf8');
			var mock = createMock(10);
			var row = [1, 2, 3];

			RedshiftBulkInsert.prototype.insert.call(mock.object, row);

			assert.strictEqual(mock.invocations[0].method, '_escapeValue');
			assert.strictEqual(mock.invocations[0].arguments[0], 1);

			assert.strictEqual(mock.invocations[1].property, 'delimiter');
			assert.strictEqual(mock.invocations[1].value, '|');
			assert.strictEqual(mock.invocations[1].operation, 'get');

			assert.strictEqual(mock.invocations[2].method, '_escapeValue');
			assert.strictEqual(mock.invocations[2].arguments[0], 2);

			assert.strictEqual(mock.invocations[3].property, 'delimiter');
			assert.strictEqual(mock.invocations[3].value, '|');
			assert.strictEqual(mock.invocations[3].operation, 'get');

			assert.strictEqual(mock.invocations[4].method, '_escapeValue');
			assert.strictEqual(mock.invocations[4].arguments[0], 3);

			assert.strictEqual(mock.invocations[5].property, '_buffer');
			assert.deepEqual(mock.invocations[5].value[0], buff);
			assert.strictEqual(mock.invocations[5].operation, 'get');

			assert.strictEqual(mock.invocations[6].property, '_currentBufferLength');
			assert.strictEqual(mock.invocations[6].value, 0);
			assert.strictEqual(mock.invocations[6].operation, 'get');

			assert.strictEqual(mock.invocations[7].property, '_currentBufferLength');
			assert.strictEqual(mock.invocations[7].value, 6);
			assert.strictEqual(mock.invocations[7].operation, 'set');

			assert.strictEqual(mock.invocations[8].property, '_buffer');
			assert.deepEqual(mock.invocations[8].value[0], buff);
			assert.strictEqual(mock.invocations[8].operation, 'get');

			assert.strictEqual(mock.invocations[9].property, '_threshold');
			assert.strictEqual(mock.invocations[9].value, 10);
			assert.strictEqual(mock.invocations[9].operation, 'get');

			assert.strictEqual(mock.invocations[10].method, '_startIdleFlushMonitor');

			RedshiftBulkInsert.prototype.insert.call(mock.object, row);

			assert.strictEqual(mock.object._buffer.length, 2);
		});

		it('flushes the buffer when the threshold is reached, returning a flush operation object', function () {
			var buff = new Buffer('1|2|3\n', 'utf8');
			var mock = createMock(1);
			var row = [1, 2, 3];

			var flushOp = RedshiftBulkInsert.prototype.insert.call(mock.object, row);

			assert.strictEqual(flushOp, mock.dummy);

			var len = mock.invocations.length;

			assert.strictEqual(mock.invocations[len - 3].method, '_stopIdleFlushMonitor');
			assert.strictEqual(mock.invocations[len - 2].method, 'flush');
			assert.strictEqual(mock.invocations[len - 1].method, '_startIdleFlushMonitor');
		});
	});

	describe('manage the idle flush monitor', function () {

		it('starts the monitor', function (done) {
			var flushCalled = false;

			var mock = {
				_idleFlushPeriod: 1000,
				flush: function () {
					flushCalled = true;
				}
			};

			RedshiftBulkInsert.prototype._startIdleFlushMonitor.call(mock);

			assert.ok(typeof(mock._timeoutRef) === 'object');

			setTimeout(function () {
				assert.strictEqual(flushCalled, false, 'flush should not have been called yet');

				setTimeout(function () {
					assert.strictEqual(flushCalled, true, 'flush should not have been called by now');
					done();
				}, 400);
			}, 800);
		});

		it('does not restart it if it was already started', function () {

			var mock = {
				_idleFlushPeriod: 1000,
				flush: function () {}
			};

			RedshiftBulkInsert.prototype._startIdleFlushMonitor.call(mock);

			assert.strictEqual(typeof(mock._timeoutRef), 'object');

			var expectedRef = mock._timeoutRef;

			RedshiftBulkInsert.prototype._startIdleFlushMonitor.call(mock);

			assert.strictEqual(mock._timeoutRef, expectedRef);
		});

		it('stops the monitor', function () {
			var mock = {
				_timeoutRef: {}
			};

			RedshiftBulkInsert.prototype._stopIdleFlushMonitor.call(mock);

			assert.strictEqual(mock._timeoutRef, undefined);
		});

		it('stops the monitor when a flush operation occurs', function () {
			var stopCalled = false;
			var mock = {
				_idleFlushPeriod: 1000,
				flush: function () { return {}; },
				_buffer: [],
				_threshold: 1,
				_stopIdleFlushMonitor: function () {
					stopCalled = true;
				},
				_startIdleFlushMonitor: function () {},
				_escapeValue: function(v) { return v; }
			};

			RedshiftBulkInsert.prototype.insert.call(mock, ['123']);

			assert.strictEqual(stopCalled, true);
		});
	});

	describe('flushing', function () {

		it('only happens if buffer has elements in it', function () {

			var mock = createMock(1);

			var flushOp = RedshiftBulkInsert.prototype.flush.call(mock.object);

			assert.strictEqual(flushOp, undefined);
		});

		it('resets the state of the buffer', function () {
			var mock = createMock(2);

			mock.createProperty('_buffer', [ '1|2|3\n', '1|2|3\n' ]);
			mock.createProperty('_currentBufferLength', 9999);

			var flushOp = RedshiftBulkInsert.prototype.flush.call(mock.object);

			assert.strictEqual(mock.object._buffer.length, 0);
			assert.strictEqual(mock.object._currentBufferLength, 0);
			assert.strictEqual(mock.object.activeFlushOps, 1);
		});

		it('"copies" the current state of the buffer to a flushop object using the factory method _newFlushOperation', function () {

			var mock = createMock(2);
			var expectedBuffer = [ '1|2|3\n', '1|2|3\n' ];
			var expectedBufferLength = 9999;

			mock.createProperty('_buffer', expectedBuffer );
			mock.createProperty('_currentBufferLength', expectedBufferLength);

			var flushOp = RedshiftBulkInsert.prototype.flush.call(mock.object);
			assert.strictEqual(flushOp, mock.dummy);

			mock.invocations.pop();
			mock.invocations.pop();

			var actual = mock.invocations.pop().arguments;

			assert.strictEqual(actual[0], expectedBuffer);
			assert.deepEqual(actual[0], expectedBuffer);
			assert.strictEqual(actual[1], expectedBufferLength);
			assert.strictEqual(actual[2], EXPECTED_KEY);
			assert.strictEqual(actual[3], EXPECTED_COPY_QUERY);
		});

		it('decreases the flush operations count when a flush operation is done', function () {

			var mock = createMock(2);

			var expectedBuffer = [ '1|2|3\n', '1|2|3\n' ];
			var expectedBufferLength = 9999;

			mock.createProperty('_buffer', expectedBuffer);
			mock.createProperty('_currentBufferLength', expectedBufferLength);

			var flushOp = RedshiftBulkInsert.prototype.flush.call(mock.object);
			assert.strictEqual(flushOp, mock.dummy);

			flushOp.emit('success');

			var actual = mock.invocations.pop();

			assert.strictEqual(actual.property, 'activeFlushOps');

			assert.strictEqual(actual.operation, 'set');

			assert.strictEqual(actual.property, 'activeFlushOps');

			assert.strictEqual(actual.value, 0);
		});

		it('decreases the flush operations count when a flush operation has errors', function () {

			var mock = createMock(2);

			var expectedBuffer = [ '1|2|3\n', '1|2|3\n' ];
			var expectedBufferLength = 9999;

			mock.createProperty('_buffer', expectedBuffer);
			mock.createProperty('_currentBufferLength', expectedBufferLength);

			var flushOp = RedshiftBulkInsert.prototype.flush.call(mock.object);
			assert.strictEqual(flushOp, mock.dummy);

			flushOp.emit('error');

			var actual = mock.invocations.pop();

			assert.strictEqual(actual.property, 'activeFlushOps');

			assert.strictEqual(actual.operation, 'set');

			assert.strictEqual(actual.property, 'activeFlushOps');

			assert.strictEqual(actual.value, 0);
		});

		it('will fail if code tries to decrease the flush op count more than once', function () {

			var mock = createMock(2);

			var expectedBuffer = [ '1|2|3\n', '1|2|3\n' ];
			var expectedBufferLength = 9999;

			mock.createProperty('_buffer', expectedBuffer);
			mock.createProperty('_currentBufferLength', expectedBufferLength);

			var flushOp = RedshiftBulkInsert.prototype.flush.call(mock.object);
			assert.strictEqual(flushOp, mock.dummy);

			flushOp.emit('error');

			var actual = mock.invocations.pop();

			assert.strictEqual(actual.property, 'activeFlushOps');

			assert.strictEqual(actual.operation, 'set');

			assert.strictEqual(actual.property, 'activeFlushOps');

			assert.strictEqual(actual.value, 0);

			try {
				flushOp.emit('error');
				throw new Error('this should have failed');
			} catch(e) {
				assert.ok(e);
			}
		});
	});

	describe('FlushOperation', function () {
		var topic = RedshiftBulkInsert.FlushOperation.prototype;

		function createFlushOperationMock() {
			var mock = PolyMock.create();

			mock.createProperty('key', EXPECTED_KEY);
			mock.createProperty('copyQuery', EXPECTED_COPY_QUERY);

			mock.b1 = new Buffer('1, 2, 3');
			mock.b2 = new Buffer('1, 2, 3');

			mock.createProperty('buffer', [mock.b1, mock.b2]);
			mock.createProperty('bufferLength', mock.b1.length + mock.b2.length);

			// creates a functor on the mock object that records invocations of returned functions
			function special(what) {
				var name = what + 'Functor';

				mock.createMethod(name, undefined);

				return function() {
					mock.object[name].apply(mock.object, Array.prototype.slice(arguments, 0));
				}
			}

			mock.createMethod('_uploadToS3', special('_uploadToS3'));
			mock.createMethod('_updateUploadLatency', special('_updateUploadLatency'));
			mock.createMethod('_updateQueryLatency', special('_updateQueryLatency'));
			mock.createMethod('_executeCopyQuery', special('_executeCopyQuery'));
			mock.createMethod('done');
			mock.createMethod('emit');

			return mock;
		}

		function createS3Mock() {
			var mock = PolyMock.create();

			mock.createMethod('put', undefined, { callbackArgs: [null, { statusCode: 200} ] });

			return mock;
		}

		it('uploads to s3', function () {
			var mock = createFlushOperationMock();
			var s3mock = createS3Mock();

			var functor = topic._uploadToS3.call(mock.object, s3mock.object);

			// call the functor
			functor(function(err, res) {});

			assert.strictEqual(s3mock.invocations[0].method, 'put');
			assert.deepEqual(s3mock.invocations[0].arguments[0], Buffer.concat([mock.b1, mock.b2]));
			assert.strictEqual(s3mock.invocations[0].arguments[1], EXPECTED_KEY);
			assert.strictEqual(typeof(s3mock.invocations[0].arguments[2]), 'function');
		});

		it('executes a copy query', function () {
			var mock = createFlushOperationMock();
			var datastoreMock = createDatastoreMock();

			var functor = topic._executeCopyQuery.call(mock.object, datastoreMock.object);

			functor('123', function(err, results) {});

			assert.strictEqual(datastoreMock.invocations[0].method, 'query');
			assert.strictEqual(datastoreMock.invocations[0].arguments[0], EXPECTED_COPY_QUERY);
		});

		it('executes an async flow when started', function () {
			var mock = createFlushOperationMock();
			var dsMock = createDatastoreMock();
			var s3Mock = createS3Mock();

			topic.start.call(mock.object, s3Mock.object, dsMock.object);


			var success = mock.invocations.pop();
			assert.strictEqual(success.method, 'success');

			// setTimeout(function () {
			// 	console.log(mock.invocations); done()
			// }, 1000)

		});


	});

	describe('old tests', function () {
		it.skip('inserts are saved in a memory buffer', function() {

			var rsbl = new RedshiftBulkInsert(null, options, awsOptions);

			rsbl.insert(testRow);
			assert.strictEqual(rsbl._currentBufferLength, new Buffer(generateExpected(1, rsbl.delimiter), 'utf8').length);
			assert.strictEqual(rsbl._buffer.length, 1);

		});

		it.skip('uploading to s3 will concatenate all the buffers', function (done) {

			var rsbl = new RedshiftBulkInsert(null, options, awsOptions);
			var mockS3 = new MockS3();
			rsbl._s3 = mockS3;

			rsbl.insert(testRow);
			rsbl.insert(testRow);

			// simulate a real flush call
			var uploadToS3 = rsbl._uploadToS3({
				filename: '999',
				buffer: rsbl._buffer
			});

			uploadToS3(function() {

				var expected = generateExpected(2, rsbl.delimiter);
				var actual = mockS3.buffer.toString('utf8');

				assert.strictEqual(actual, expected);

				done()
			});
		});


		it.skip('flushes when a predetemined amount of inserts are executed', function (done) {
			this.timeout(7000);

			var datastore = new MockDatastore();
			var s3 = new MockS3();
			var opts = _u.clone(options);

			opts.idleFlushPeriod = 1000;

			var rsbl = new RedshiftBulkInsert(datastore, opts, awsOptions);

			rsbl._s3 = s3;

			var flushCalls = 0;

			rsbl.on('flush', function(err, results, sql, start, bi) {
				assert.ok(err === null);
				assert.ok(sql);
				console.log(sql)
				flushCalls++;
			});

			setTimeout(function () {

				rsbl.insert(testRow);
				rsbl.insert(testRow);
				rsbl.insert(testRow);
				//assert.strictEqual(rsbl._timeoutRef, undefined);

				setTimeout(function() {

					assert.strictEqual(flushCalls, 1, 'only one flush should have occurred by now - triggered by threshold - but there were ' + flushCalls + ' flush calls');

					setTimeout(function () {

						assert.strictEqual(flushCalls, 1, 'although flush idle time passed, no inserts were made - so only flush should have occurred by now');

						rsbl.insert(testRow);
						assert.strictEqual(rsbl._buffer.length, 1);

						setTimeout(function () {

							assert.strictEqual(flushCalls, 2, 'two flushes were expected by now');
							done();

						}, 1100);

					}, 1100);

				}, 1100);

			}, 500);
		});
	});


});

function generateExpected(rows, delimiter) {
	var result = '';
	for (var i = 0; i < rows; i++) {
		var expected = [].concat(testRow);
		expected[expected.length - 1] = '\\N';
		expected = expected.join(delimiter);
		expected += '\n';
		result += expected;
	}

	return result;
}


function MockS3() {
	var self = this;
	this.putObject = function(opts, callback) {
		self.opts = opts;
		callback(null, {});
	}

	this.deleteObject = function(opts, callback) {
		self.deleteOpts = opts;
		callback(null, {});
	}

	this.putBuffer = function(buffer, key, callback) {
		this.buffer = buffer;
		this.key = key;
		callback(null, { statusCode: 200 });
	}

}

function MockDatastore() {
	var queries = this.queries = [];
	this.query = function(sql, callback) {
		queries.push(sql);
		callback(null, {});
	}
}

