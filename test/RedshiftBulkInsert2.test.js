var RedshiftBulkInsert = require('../lib/RedshiftBulkInsert2.js');
var assert = require('assert');
var SimpleFileWriter = require('simple-file-writer');
var fs = require('fs');
var path = require('path');
var EventEmitter = require('events').EventEmitter;
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
	region: '1', 
	accessKeyId: '2', 
	secretAccessKey: '3',
	bucket: '4'
};

describe('RedshiftBulkInsert', function() {
	
	before(cleanup);
	//after(cleanup);

	it('writes stuff directly to a file', function(done) {
		var sfw = new SimpleFileWriter();

		var rsbl = new RedshiftBulkInsert(null, sfw, options, awsOptions);

		rsbl.insert(testRow, function(err) { 
			if (err)
				throw err;

			var actual = fs.readFileSync(rsbl._filename, 'utf8');			
			var expected = generateExpected(1, rsbl.delimiter);
			assert.strictEqual(actual, expected);

			assert.strictEqual(rsbl._inserts, 1);

			done();
		});		
	});	

	it('writes each insert in its own row', function (done) {
		var sfw = new SimpleFileWriter();

		var rsbl = new RedshiftBulkInsert(null, sfw, options, awsOptions);

		rsbl.insert(testRow, function(err) { 
			if (err)
				throw err;

			rsbl.insert(testRow, function(err) {
				if (err)
					throw err;

				var actual = fs.readFileSync(rsbl._filename, 'utf8');			
				var expected = generateExpected(2, rsbl.delimiter);
				assert.strictEqual(actual, expected);
				done();
			});
		});
	})
	
	it('FlushOperation', function (done) {
		var mock = new MockRedshiftBulkInsert();
		var op = new RedshiftBulkInsert.FlushOperation(mock, 'test');

		mock._inserts = 1;

		mock.on('flush', function(err, results, sql, start, bi) {
			assert.strictEqual(mock.activeFlushOps, 0);
			assert.strictEqual(mock._inserts, 0);
			assert.strictEqual(start, op._start);
			assert.ok(mock._readFileCalled, 1);
			assert.ok(mock._uploadFileToS3Called, 2);
			assert.ok(mock._executeCopyQuery, 3);
			done();
		});

		op.start();

		assert.ok(mock.rotateFilenameCalled);
		assert.strictEqual(mock.activeFlushOps, 1);
	});
	

	it('flushes when a predetemined amount of inserts is done', function (done) {
		this.timeout(20000);

		var sfw = new SimpleFileWriter();
		var datastore = new MockDatastore();
		var s3 = new MockS3();
		var opts = _u.clone(options);
		
		opts.idleFlushPeriod = 1000;

		var rsbl = new RedshiftBulkInsert(datastore, sfw, opts, awsOptions);

		rsbl._s3 = s3;		

		var flushCalls = 0;

		rsbl.on('flush', function(err, results, sql, start, bi) {			
			flushCalls++;
		});

		setTimeout(function () {

			rsbl.insert(testRow);
			rsbl.insert(testRow);
			rsbl.insert(testRow, function() {
				assert.strictEqual(rsbl._timeoutRef, undefined);
			});
			
			setTimeout(function() {

				assert.strictEqual(flushCalls, 1, 'only one flush should have occurred by now - triggered by threshold');
				
				setTimeout(function () {
	
					assert.strictEqual(flushCalls, 1, 'although flush idle time passed, no inserts were made - so only flush should have occurred by now');

					rsbl.insert(testRow, function() {
						
						assert.strictEqual(rsbl._inserts, 1);						

						setTimeout(function () {
							assert.strictEqual(rsbl._inserts, 0);
							assert.strictEqual(flushCalls, 2, 'two flushes were expected by now');
							done();
						}, 1100);

					});					
					
				}, 1100);

			}, 1100);

		}, 500);
		
	});

	it('flushes when a predetermined period of time passes and inserts didnt trigger a flush by quantity', function(done) {
		var sfw = new SimpleFileWriter();
		var datastore = new MockDatastore();
		var s3 = new MockS3();

		var rsbl = new RedshiftBulkInsert(datastore, sfw, options, awsOptions);

		rsbl._s3 = s3;		

		rsbl.on('flush', function(err, results, sql, start, bi) {

			if (err)
				throw err;

			assert.strictEqual(datastore.queries.length, 1);
			assert.strictEqual(datastore.queries[0], sql);
			done();			
		});

		rsbl.insert(testRow);
		rsbl.insert(testRow);
		rsbl.insert(testRow);
	});
});

function MockDatastore() {
	var queries = this.queries = [];
	this.query = function(sql, callback) {
		queries.push(sql);
		callback(null);
	}
}

$u.inherits(MockRedshiftBulkInsert, EventEmitter);
function MockRedshiftBulkInsert() {
	EventEmitter.call(this);
	this.activeFlushOps = 0;
	var self = this;
	this._rotateFilename = function() {
		self.rotateFilenameCalled = true;
	};

	var calls = 0;

	this._readFile = function(flushOp) {
		return function(callback) {
			self._readFileCalled = ++calls
			callback(null);
		}
	};

	this._uploadFileToS3 = function(flushOp) {
		return function(callback) {
			self._uploadFileToS3Called = ++calls
			callback(null);	
		}
	};

	this._executeCopyQuery = function(flushOp) {
		return function(callback) {
			self._executeCopyQueryCalled = ++calls
			callback(null);
		}
	};
}

function MockS3() {
	var self = this;
	this.putObject = function(opts, callback) {						
		self.opts = opts;
		callback();
	}
}

function MockFlushOperation() {
	var self = this;
	this.start = function() {
		self.startCalled = true;
	}
}

function cleanup() {
	if (fs.existsSync(testroot)) {			

		var files = fs.readdirSync(testroot);
		
		for (var i = 0; i < files.length; i++)
			fs.unlinkSync(path.join(testroot, files[i]));

		fs.rmdirSync(testroot);		
	}

	fs.mkdirSync(testroot);	
}

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