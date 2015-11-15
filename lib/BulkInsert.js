var Insert = require('./Insert');
var inherits = require('util').inherits;
var isArray = require('util').isArray;

module.exports = BulkInsert;

function BulkInsert(datastore, table, fields) {
	if (!(this instanceof BulkInsert)) return new BulkInsert(datastore, table, fields);
	Insert.call(this, datastore, table, fields);
}

inherits(BulkInsert, Insert);

BulkInsert.prototype.execute = function(rows, callback) {

	if (!isArray(rows)) throw new Error('rows argument must be an array of arrays');

	if (rows.length === 0) throw new Error('rows argument must contain atleast one row');

	var query = this.sqlBase + ' (' + rows.join('),\n(') + ')';

	this.datastore.query(query, callback);
}
