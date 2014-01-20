var assert = require('assert');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var BulkInsertMonitor = require('../lib/BulkInsertMonitor');

function MockClient() {
	this.events = [];
}

MockClient.prototype.send = function(event) {
	this.events.push(event);
};

MockClient.prototype.Event = function(data) {
	return data;
};

util.inherits(MockBulkInsert, EventEmitter);
function MockBulkInsert() {
	EventEmitter.call(this);
	this.activeFlushOps = 1;
}

//TODO add queryLatency and uploadLatency tests

describe('monitor a bulk insert instance', function() {

	var monitor = new BulkInsertMonitor({
		role: 'test',
		ttl: 1,
		flushLatencyThreshold: 1,
		activeFlushOpsThreshold: 1
	});

	var client = new MockClient();
	monitor.bindClient(client);

	var bulkInsert = new MockBulkInsert();

	it('reports when a flush occurs', function() {

		bulkInsert.once('flush', monitor.flushLatencyListener());
		bulkInsert.once('flush', monitor.activeFlushOperationsListener());
		bulkInsert.emit('flush', null, [], '', Date.now(), bulkInsert);

		assert.strictEqual(client.events.length, 2);
	});

});
