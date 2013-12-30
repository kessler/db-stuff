var assert = require('assert');
var BulkInsertRetryPolicy = require('../lib/BulkInsertRetryPolicy.js');
var PolyMock = require('polymock');
var EventEmitter = require('events').EventEmitter;

function createRSBLMock() {
	var mock = PolyMock.create();

	mock.emitter = new EventEmitter();

	mock.createMethod('retryFlush');

	mock.createMethod('on', undefined, { invokeCallback:false, dynamicValue: function(event, handler) {
		mock.emitter.on(event, handler);
	}});

	return mock;
}

function createFlushOpMock() {
	var mock = PolyMock.create();

	mock.emitter = new EventEmitter();

	mock.createMethod('once', undefined, { invokeCallback:false, dynamicValue: function(event, handler) {
		mock.emitter.on(event, handler);
	}});

	return mock;
}

describe('BulkInsertRetryPolicy', function () {

	it('merges user provided options with defaults', function () {

		var mock = createRSBLMock();

		var options = { maxRetries: 10 };

		var emitter = BulkInsertRetryPolicy.ephemeralRetryPolicy(mock.object, options);

		assert.strictEqual(options.maxRetries, 10);
		assert.strictEqual(options.retryCalculation, BulkInsertRetryPolicy.defaults.retryCalculation);
		assert.strictEqual(options.timeSlot, BulkInsertRetryPolicy.defaults.timeSlot);
		assert.strictEqual(options.maxDelay, BulkInsertRetryPolicy.defaults.maxDelay);
	});

	it('listens to flush ops starting on a bulkinsert instance', function () {
		var mock = createRSBLMock();

		var emitter = BulkInsertRetryPolicy.ephemeralRetryPolicy(mock.object);

		assert.strictEqual(mock.invocations[0].method, 'on');
		assert.strictEqual(mock.invocations[0].arguments[0], 'flush');
		assert.strictEqual(typeof(mock.invocations[0].arguments[1]), 'function');
	});

	it('listens to an error event (only once) when a flush operation starts', function () {
		var mock = createRSBLMock();
		var emitter = BulkInsertRetryPolicy.ephemeralRetryPolicy(mock.object);

		var flushOpMock = createFlushOpMock();

		mock.emitter.emit('flush', flushOpMock.object);

		assert.strictEqual(flushOpMock.invocations[0].method, 'once');
		assert.strictEqual(flushOpMock.invocations[0].arguments[0], 'error');
		assert.strictEqual(typeof(flushOpMock.invocations[0].arguments[1]), 'function');
	});

	it('retries the operation (sometimes with a delay) when an error event is raised by a flush op instance', function (done) {

		var mock = createRSBLMock();
		var flushOpMock = createFlushOpMock();
		var emitter = BulkInsertRetryPolicy.ephemeralRetryPolicy(mock.object);

		emitter.on('next flush', function(flushOp, job) {
			assert.strictEqual(flushOp, flushOpMock.object);
			done();
		});

		emitter.on('no more retries', function (flushop) {
			done('no more retries every should not have been raised');
		});

		mock.emitter.emit('flush', flushOpMock.object);

		flushOpMock.emitter.emit('error', 'damn!', flushOpMock.object);
	});

	it('stops after a predefined number of retries', function (done) {
		this.timeout(2500);
		var mock = createRSBLMock();
		var flushOpMock = createFlushOpMock();

		var options = {
			timeSlot: 2,
			retryCalculation: BulkInsertRetryPolicy.exponentialBackoff,
			maxRetries: 3
		};

		var emitter = BulkInsertRetryPolicy.ephemeralRetryPolicy(mock.object, options);

		var retries = 0;

		emitter.on('next flush', function(flushOp, job) {
			assert.strictEqual(flushOp, flushOpMock.object);
			assert.ok(++retries <= 3);
		});

		emitter.on('no more retries', function (flushop) {
			done('no more retries should not have been raised');
		});

		mock.emitter.emit('flush', flushOpMock.object);

		flushOpMock.emitter.emit('error', 'damn!', flushOpMock.object);
		flushOpMock.emitter.emit('error', 'damn!', flushOpMock.object);
		flushOpMock.emitter.emit('error', 'damn!', flushOpMock.object);

		setTimeout(function() {
			assert.strictEqual(retries, 3);
			done();
		}, 2000);
	});

});