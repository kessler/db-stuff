var EventEmitter = require('events').EventEmitter;
var random = require('./random.js');
var _l = require('lodash');

var MAX = math.pow(2, 53);

var defaults = {
	timeSlot: 1000, //default time slot is one second
};

//Exponential backoff: http://en.wikipedia.org/wiki/Exponential_backoff
module.exports.exponentialBackoff = function(attemps, timeSlot) {

	// wait anywhere between zero to 2^attempts inclusive (hence +1)
	var timeSlots = random(0, math.pow(2, attemps) + 1);

}



module.exports.backoffEphemeralPolicy = function(bulkInsert, options) {

	function onFlushOpError(error, flushOp) {

	}

	function onFlush(flushOp) {
		flushOp.on('error', onFlushOpError);
	}

	bulkInsert.on('flush')

	var policyEmitter = new EventEmitter();

	var attemps = 0;

	flushOp.on('error', function(err) {


		if (maxRetries > -1 && ++attemps <= maxRetries) {
			//policyEmitter.emit('retry', );
		}

	});

	return policyEmitter;
};