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