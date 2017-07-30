var $u = require('util')
var Insert = require('./Insert')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var BulkInsert = require('./BulkInsert')

/*
	base class for datastores
*/
inherits(DatastoreBase, EventEmitter)
function DatastoreBase(config) {
	EventEmitter.call(this)
	this.config = config
}

function notImplemented() {
	throw new Error('not implemented')
}

DatastoreBase.prototype.query = notImplemented
DatastoreBase.prototype.create = notImplemented
DatastoreBase.prototype.end = notImplemented

DatastoreBase.prototype.insert = function(table, object, callback) {

	var fields = Object.keys(object)
	
	var sql = 'INSERT INTO ' + table + this._generateFieldSql(fields) + ' VALUES ('

	for (var i = 0; i < fields.length; i++) {
		if (i > 0)
			sql += ','

		sql += this.formatValue(object[fields[i]])
	}

	sql += ');'

	this.query(sql, callback)
}

DatastoreBase.prototype.update = function (table, object, filter, callback) {
	
	if (typeof (filter) === 'function') {
		callback = filter
		filter = undefined
	}

	var sql = 'UPDATE ' + table + ' SET '
	
	var count = 0
	for (var f in object) {
		if (count++ > 0)
			sql += ','

		sql += f + '=' + this.formatValue(object[f])
	}

	if (filter) {
		sql += ' WHERE '

		count = 0
		for (var field in filter) {
			if (count++ > 0)
				sql += ' AND '

			sql += this.formatField(field) + '=' + this.formatValue(filter[field])
		}
	}

	sql += ';'

	this.query(sql, callback)
}


DatastoreBase.prototype.formatValue = function(val) {
	if (typeof(val) === 'undefined' || val === null) 
		return "null"

	if (typeof(val) === 'number')
		return val
	
	return "'" + val + "'"
}

DatastoreBase.prototype.formatField = function(field) {
	return field.replace(/[^a-z0-9]/ig, '_')
}

DatastoreBase.prototype._generateFieldSql = function(fields) {
	
	var result = ' ('

	for (var i = 0; i < fields.length; i++) {
		if (i > 0)
			result += ',';

		result += this.formatField( fields[i] )
	}

	return result + ')';
}

DatastoreBase.prototype.newInsertCommand = function(table, fields) {
	return new Insert(this, table, fields)
}

DatastoreBase.prototype.newBulkInsertCommand = function(table, fields) {
	return new BulkInsert(this, table, fields)
}

module.exports = DatastoreBase
