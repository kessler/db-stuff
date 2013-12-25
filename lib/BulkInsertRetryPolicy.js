var EventEmitter = require('events').EventEmitter;
var random = require('./random.js');
var _l = require('lodash');

var MAX = Math.pow(2, 53);

var defaults = {
	timeSlot: 1000, //default time slot is one second
	backoffAlgorithm: exports.randomExponentialBackoff
};

//Exponential backoff: http://en.wikipedia.org/wiki/Exponential_backoff
module.exports.randomExponentialBackoff = function(attempts) {

	// wait anywhere between zero to 2^attempts inclusive (hence +1)
	return random(0, Math.pow(2, attemps) + 1);
};

/*
	an exponential backoff policy that
*/
module.exports.backoffEphemeralPolicy = function(bulkInsert, options) {

	var attempts = 0;

	var policyEmitter = new EventEmitter();

	function onFlushOpError(error, flushOp) {
		var timeSlots = exports.exponentialBackoff(++attempts);

		var delay = timeSlots * options.timeSlot;

		var nextAttempt = Date.now() + delay;

		setTimeout(function () {
			bulkInsert.retryFlush(flushOp);
		}, delay)

		policyEmitter.emit('next attempt', nextAttempt);
	}

	function onFlush(flushOp) {
		flushOp.on('error', onFlushOpError);
	}

	bulkInsert.on('flush', onFlush);

	return policyEmitter;
};