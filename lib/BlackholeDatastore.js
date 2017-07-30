var $u = require('util')
var DatastoreBase = require('./DatastoreBase')
var EventEmitter = require('events').EventEmitter

/*
 	a datastore that does nothing but send stuff to oblivion
*/

$u.inherits(BlackholeDatastore, DatastoreBase)
function BlackholeDatastore() {
	DatastoreBase.call(this)	
}

BlackholeDatastore.prototype.create = function(callback) {
	var self = this

	if (callback) {
		process.nextTick(function() {		
			callback(null, self)
		})
	}
}

BlackholeDatastore.prototype.query = function(sql, values, callback) {

	if (typeof(values) === 'function') {
		callback = values
		values = undefined
	}

	if (callback) {		
		process.nextTick(function() {			
			callback(null, [])
		})		
	}
}

BlackholeDatastore.prototype.createQuery = function(sql, values, callback) {

	if (typeof(values) === 'function') {
		callback = values
		values = undefined
	}

	process.nextTick(function() {		
		callback(null, new DevQuery())
	})
}


$u.inherits(DevQuery, EventEmitter)
function DevQuery() {
	EventEmitter.call(this)
}

module.exports = BlackholeDatastore