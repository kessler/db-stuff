var $u = require('util')
var DatastoreBase = require('./DatastoreBase')
var EventEmitter = require('events').EventEmitter

$u.inherits(DevelopmentDatastore, DatastoreBase)

function DevelopmentDatastore(config) {
	DatastoreBase.call(this)
	this.queries = []

	if (config) {
		this.syntheticDelay = config.syntheticDelay
	}
}

DevelopmentDatastore.prototype.create = function(callback) {
	var self = this

	if (callback) {
		process.nextTick(function() {
			callback(null, self)
		})
	}
}

DevelopmentDatastore.prototype.query = function(sql, values, callback) {

	if (typeof(values) === 'function') {
		callback = values
		values = undefined
	}

	this.queries.push(sql)

	function callbk() {
		callback(null, [])
	}

	if (callback) {

		if (this.syntheticDelay) {
			setTimeout(callbk, this.syntheticDelay)
		} else {
			process.nextTick(callbk)
		}
	}
}

DevelopmentDatastore.prototype.createQuery = function(sql, values, callback) {

	if (typeof(values) === 'function') {
		callback = values
		values = undefined
	}

	this.queries.push(sql)

	function callbk() {
		callback(null, new DevQuery())
	}

	if (callback) {

		if (this.syntheticDelay) {
			setTimeout(callbk, this.syntheticDelay)
		} else {
			process.nextTick(callbk)
		}
	}
}


$u.inherits(DevQuery, EventEmitter)

function DevQuery() {
	EventEmitter.call(this)
}

module.exports = DevelopmentDatastore
