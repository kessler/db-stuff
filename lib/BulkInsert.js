var $u = require('util');
var EventEmitter = require('events').EventEmitter;

var defaultParams = {
	threshold: 250, 
	idleFlushPeriod: 5000
};

function mergeParams(params) {
	if (params === undefined) {
		return defaultParams;
	}

	for (var p in defaultParams) {
		if (typeof(params[p]) === 'undefined') {
			params[p] = defaultParams[p];
		}
	}

	return params;
}

//TODO: might be some common functionality that can be used with Insert in a sensible way?
function BulkInsert(datastore, table, paramsOrFields, fields) {
	if (typeof(datastore) === 'undefined')
		throw new Error('missing datastore parameter');

	if (typeof(table) === 'undefined')
		throw new Error('missing table parameter');
	
	EventEmitter.call(this);

	var params = paramsOrFields;
	if ($u.isArray(paramsOrFields)) {
		fields = paramsOrFields;
		params = undefined;
	}

	this.datastore = datastore;
	this.params = mergeParams(params);
	this.table = table;
	this.sqlBase = 'insert into ' + table;
	
	if ($u.isArray(fields)) {		
		if (fields.length === 0)
			throw new Error('cannot use empty array for fields');

		this.sqlBase += this.datastore._generateFieldSql(fields);		
		this.fields = fields;
		this.fieldCount = fields.length;
	} else {
		throw new Error('cannot use empty array for fields');
	}

	this.sqlBase += ' values ';
	this.buffer = [];
	this.activeFlushOps = 0;
	this.startIdleFlushMonitor();
}

$u.inherits(BulkInsert, EventEmitter);

BulkInsert.prototype.flush = function() {
	var self = this;

	if (self.buffer.length > 0) {
		var sql = self.sqlBase + self.buffer.join(",");
		
		var flushStart = Date.now();
		self.activeFlushOps++;
		self.datastore.query(sql, function(err, results) {			
			self.activeFlushOps--;
			self.emit('flush', err, results, sql, flushStart, self);
		});	

		self.buffer = [];
	}

	self.startIdleFlushMonitor();
};

BulkInsert.prototype.close = function () {
	clearTimeout(this.ref);
};


BulkInsert.prototype.startIdleFlushMonitor = function () {
	var self = this;

	self.ref = setTimeout(function() {
		
		self.flush();

	}, self.params.idleFlushPeriod);
};

BulkInsert.prototype.insert = function(row) {
	var text = '';
	
	if ($u.isArray(row)) {

		if (row.length !== this.fieldCount) {
			var err = 'the number of values in this row ' + $u.inspect(row) + ' do not match the preset fieldcount of ' + this.fieldCount + ' for this bulk insert';
			throw new Error(err);
		}

		for (var i = 0; i < row.length; i++) {
			if (i > 0)
				text += ',';

			text += this.datastore.formatValue(row[i]);
		}

	} else {
		throw new Error('cannot use ' + row);
	}

	if (row.length > 0) {
		text = '(' + text + ')';
	}

	this.buffer.push(text);

	if (this.buffer.length === this.params.threshold) {
		clearTimeout(this.ref);
		this.flush();
	}
};

module.exports = BulkInsert;