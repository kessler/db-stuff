var assert = require('assert');
var BulkInsertRetryPolicy = require('../lib/BulkInsertRetryPolicy.js');
var PolyMock = require('polymock');
var EventEmitter = require('events').EventEmitter;

function createRSBLMock() {
	var mock = PolyMock.create();

	mock.emitter = new EventEmitter();

	mock.createMethod('retryFlush');
	mock.createMethod('on', function(event, handler) {
		mock.emitter.on(event, handler);
	});

	return mock;
}

describe('BulkInsertRetryPolicy', function () {

	it('executes a backoff policy on a bulkinsert instance', function () {
		var mock = createRSBLMock();
		var emitter = BulkInsertRetryPolicy.backoffEphemeralPolicy(mock);





	});

});