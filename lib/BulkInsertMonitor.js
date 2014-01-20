"use strict";

var Monitor = require('riemann-util').RiemannMonitor;
var util = require('util');

util.inherits(BulkInsertMonitor, Monitor);
function BulkInsertMonitor(config) {
	Monitor.call(this, config);

	if (config.flushLatencyThreshold === undefined)
		throw new Error('missing flushLatencyThreshold in configuration');

	if (config.activeFlushOpsThreshold === undefined)
		throw new Error('missing activeFlushOpsThreshold in configuration');
}

//backward compatibility
BulkInsertMonitor.prototype.flushLatencyListener = BulkInsertMonitor.prototype.bulkInsert_flushLatencyListener = function(event) {
	//var ttl = 60 * 60;
	var ttl = event.ttl;
	var eventName = event.name;
	var self = this;
	var config = this.config;

	return function(err, results, sql, flushStart) {
		var now = Date.now();
		var metric = now - flushStart;
		var state;

		if (metric > config.flushLatencyThreshold * 3)
			state = 'critical';
		else if (metric > config.flushLatencyThreshold)
			state = 'warning';
		else
			state = 'ok';

		self.send({
			service:'bulk insert flush latency',
			metric: metric,
			state: state,
			ttl: ttl,
			tags: ['performance', 'database', eventName]
		});
	};
};

//backward compatibility
BulkInsertMonitor.prototype.activeFlushOperationsListener = BulkInsertMonitor.prototype.bulkInsert_activeFlushOperationsListener = function(event) {
	//var ttl = 60 * 60;
	var ttl = event.ttl;
	var eventName = event.name;
	var self = this;
	var config = this.config;

	return function(err, results, sql, flushStart, bulkInsert) {

		var metric = bulkInsert.activeFlushOps;
		var state;

		if (metric > config.activeFlushOpsThreshold * 3)
			state = 'critical';
		else if (metric > config.activeFlushOpsThreshold)
			state = 'warning';
		else
			state = 'ok';

		self.send({
			service: 'concurrent flush operations',
			metric: metric,
			state: state,
			ttl: ttl,
			tags: ['performance', 'database', eventName]
		});
	};
};

BulkInsertMonitor.prototype.copyQueryLatencyListener = function(event) {
	//var ttl = 60 * 60;
	var ttl = event.ttl;
	var eventName = event.name;
	var self = this;
	var config = this.config;

	return function(flushOp) {

		var metric = flushOp.queryLatency;
		var state;

		if (metric > config.queryLatencyThreshold * 2)
			state = 'critical';
		else if (metric > config.queryLatencyThreshold)
			state = 'warning';
		else
			state = 'ok';

		self.send({
			service: 'copy query latency',
			metric: metric,
			state: state,
			ttl: ttl,
			tags: ['performance', 'database', eventName]
		});
	};
};

BulkInsertMonitor.prototype.uploadLatencyListener = function(event) {
	//var ttl = 60 * 60;
	var ttl = event.ttl;
	var eventName = event.name;
	var self = this;
	var config = this.config;

	return function(flushOp) {

		var metric = flushOp.uploadLatency;
		var state;

		if (metric > config.queryLatencyThreshold * 2)
			state = 'critical';
		else if (metric > config.queryLatencyThreshold)
			state = 'warning';
		else
			state = 'ok';

		self.send({
			service: 's3 upload latency',
			metric: metric,
			state: state,
			ttl: ttl,
			tags: ['performance', 'database', eventName]
		});
	};
};

module.exports = BulkInsertMonitor;