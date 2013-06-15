var $u = require('util');
var DatastoreBase = require('./DatastoreBase');
var EventEmitter = require('events').EventEmitter;

$u.inherits(DevelopmentDatastore, DatastoreBase);
function DevelopmentDatastore() {
	DatastoreBase.call(this);
	this.queries = [];
}

DevelopmentDatastore.prototype.create = function(callback) {
	var self = this;

	if (callback) {
		process.nextTick(function() {		
			callback(null, self);
		});
	}
};

DevelopmentDatastore.prototype.query = function(sql, values, callback) {

	if (typeof(values) === 'function') {
		callback = values;
		values = undefined;
	}

	this.queries.push(sql);

	if (callback) {		
		process.nextTick(function() {			
			callback(null, []);
		});		
	}
};

DevelopmentDatastore.prototype.createQuery = function(sql, values, callback) {

	if (typeof(values) === 'function') {
		callback = values;
		values = undefined;
	}

	this.queries.push(sql);

	process.nextTick(function() {		
		callback(null, new DevQuery());
	});
};


$u.inherits(DevQuery, EventEmitter);
function DevQuery() {
	EventEmitter.call(this);
}

module.exports = DevelopmentDatastore;