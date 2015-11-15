var $u = require('util');

function Insert(datastore, table, fields) {
	if (datastore === undefined) {
		throw new Error('missing datastore parameter');
	}

	if (table === undefined) {
		throw new Error('missing table parameter');
	}

	this.datastore = datastore;
	this.table = table;
	this.sqlBase = 'insert into ' + table;

	if ($u.isArray(fields)) {
		if (fields.length === 0) {
			throw new Error('cannot use empty array for fields');
		}
		
		this.sqlBase += this.datastore._generateFieldSql(fields);	
		this.fields = fields;
		this.fieldCount = fields.length;
	}

	this.sqlBase += ' values ';
}

Insert.prototype._formatRow = function (row) {
	var text = ''

	if (this.fieldCount !== undefined && row.length !== this.fieldCount) {
		var err = 'the number of values in this row ' + $u.inspect(row) + ' do not match the preset fieldcount of ' + this.fieldCount + ' for this bulk insert';
		throw new Error(err);
	}

	for (var i = 0; i < row.length; i++) {
		if (i > 0)
			text += ',';

		text += this.datastore.formatValue(row[i]);
	}

	return text
}

Insert.prototype.execute = function(row, callback) {
	var text = '';

	if ($u.isArray(row)) {
		text += this._formatRow(row)
	} else if (typeof(row) === 'string') {
		text = this.datastore.formatValue(row);
	} else {
		throw new Error('cannot use ' + row);
	}

	if (row.length > 0) {
		text = '(' + text + ')';
	}

	var sql = this.sqlBase + text;
	this.datastore.query(sql, callback);
};

module.exports = Insert;