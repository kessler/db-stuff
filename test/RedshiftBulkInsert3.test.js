var RedshiftBulkInsert = require('../lib/RedshiftBulkInsert3.js');
var assert = require('assert');
var fs = require('fs');
var path = require('path');
var $u = require('util');
var _u = require('underscore');

var dbStuff = require('../index.js');

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

describe('RedshiftBulkInsert', function() {

	describe('batch inserts', function () {

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

	describe('old tests', function () {
		it('inserts are saved in a memory buffer', function() {

			var rsbl = new RedshiftBulkInsert(null, options, awsOptions);

			rsbl.insert(testRow);
			assert.strictEqual(rsbl._currentBufferLength, new Buffer(generateExpected(1, rsbl.delimiter), 'utf8').length);
			assert.strictEqual(rsbl._buffer.length, 1);

		});

		it('uploading to s3 will concatenate all the buffers', function (done) {

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


		it('flushes when a predetemined amount of inserts are executed', function (done) {
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