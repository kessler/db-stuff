var $u = require('util');
var Insert = require('./Insert');


/*
	base class for datastores
*/
function DatastoreBase(config) {
	this.config = config;
}

function notImplemented() {
	throw new Error('not implemented');
}

DatastoreBase.prototype.query = notImplemented;
DatastoreBase.prototype.create = notImplemented;

DatastoreBase.prototype.insert = function(table, object, callback) {

	var fields = Object.keys(object)
	
	var sql = 'INSERT INTO ' + table + ' (' + fields.join(',') + ') VALUES ('

	for (var i = 0; i < fields.length; i++) {
		if (i > 0)
			sql += ','

		sql += this.formatValue(object[fields[i]])
	}

	sql += ');'
	
	console.log(sql)
	this.query(sql, callback)
};

DatastoreBase.prototype.update = function (table, object, filter, callback) {
	
	if (typeof (filter) === 'function') {
		callback = filter
		filter = undefineds
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

			sql += field + '=' + this.formatValue(filter[field])
		}
	}

	sql += ';'

	console.log(sql)

	this.query(sql, callback)
};

DatastoreBase.prototype.formatValue = function(val) {
	if (typeof(val) === 'undefined' || val === null) 
		return 'null';

	if (typeof(val) === 'number')
		return val;

	if ($u.isArray(val))
		return createDollarQuotedString(val.join(','));
	
	return createDollarQuotedString(val);
}

DatastoreBase.prototype._generateFieldSql = function(fields) {
	
	var result = ' (';

	for (var i = 0; i < fields.length; i++) {
		if (i > 0)
			result += ',';

		result += fields[i];
	}

	return result + ')';
};

DatastoreBase.prototype.newInsertCommand = function(table, fields) {
	return new Insert(this, table, fields);
};


DatastoreBase.prototype.formatValue = function(val) {
	if (typeof(val) === 'undefined' || val === null) 
		return "null";

	if (typeof(val) === 'number')
		return val;
	
	return "'" + val + "'";
};

module.exports = DatastoreBase;