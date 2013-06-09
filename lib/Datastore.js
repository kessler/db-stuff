var pg = require('pg');
var EventEmitter = require('events').EventEmitter;
var $u = require('util');

var Insert = require('./Insert');

var logger = require('log4js').getLogger('Datastore');


function random(start, end) {
    var range = end - start;
    return Math.floor((Math.random() * range) + start);
}


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





$u.inherits(PostgresDatastore, DatastoreBase);
function PostgresDatastore(config) {
	DatastoreBase.call(this, config);
}

PostgresDatastore.prototype.create = function(callback) {
	pg.defaults.poolSize = this.config.poolSize || 5;
	var self = this;
	pg.connect(this.config.connectionString, function(err, client, done) {
		if (err === null) {
			client.query('select 1=1', function(err1, results) {			
				done();
				callback(err1, self);				
			});
		} else {
			callback(err);
		}
	});		
};

/*
	run the query and execute callback with results
*/
PostgresDatastore.prototype.query = function(sql, values, callback) {	
	
	if (typeof(values) === 'function') {
		callback = values;
		values = undefined;
	}

	pg.connect(this.config.connectionString, function(err, client, done) {
		if (err === null) {

			var query = client.query(sql, values, callback);
			
			query.on('end', function () {
				done();
			});

		} else {
			done(err);
			callback(err);
		}
	});		
};

/*
	run the query and execute callback with a query instance
*/
PostgresDatastore.prototype.createQuery = function(sql, values, callback) {
	
	if (typeof(values) === 'function') {
		callback = values;
		values = undefined;
	}

	pg.connect(this.config.connectionString, function(err, client, done) {
		if (err === null) {

			var query = client.query(sql, values);
			
			query.on('end', function () {
				done();
			});

			callback(null, query);
		} else {
			done(err);
			callback(err);
		}
	});		
};

/*
	pg Dollar-Quoted String Constants - http://www.postgresql.org/docs/8.3/interactive/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING 
*/
var randomTags = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'g', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't'];

function createDollarQuotedString(val) {

	var randomTag = '$'+ randomTags[ random(0, randomTags.length) ] +'$';

	return randomTag + val + randomTag;
}

PostgresDatastore.prototype.formatValue = function(val) {
	if (typeof(val) === 'undefined' || val === null) 
		return 'null';

	if (typeof(val) === 'number')
		return val;

	if ($u.isArray(val))
		return createDollarQuotedString(val.join(','));
	
	return createDollarQuotedString(val);
}




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

DevelopmentDatastore.prototype.query = function(sql, callback) {
	this.queries.push(sql);

	if (callback) {		
		process.nextTick(function() {			
			callback(null, []);
		});		
	}
};

DevelopmentDatastore.prototype.createQuery = function(sql, callback) {
	process.nextTick(function() {
		callback(null, new DevQuery());
	});
};


$u.inherits(DevQuery, EventEmitter);
function DevQuery() {
	EventEmitter.call(this);
}

module.exports.DatastoreBase = DatastoreBase;
module.exports.DevelopmentDatastore = DevelopmentDatastore;
module.exports.PostgresDatastore = PostgresDatastore;

/*
	factory for creating datastores.

	usage:

	var ds = Datastore.create( { implementation: 'SomeImplementation', additional config params... }, myCallback);

	var ds = Datastore.create( { implementation: 'SomeImplementation', additional config params... });

	var ds = Datastore.create('SomeImplementation', myCallback);

	var ds = Datastore.create('SomeImplementation');
*/
module.exports.create = function(config, callback) {
	var ds;

	var implementation
	
	if (typeof(config) === 'string')
		implementation = config;
	else
		implementation = config.implementation;

	if (implementation === 'PostgresDatastore') {
		ds = new PostgresDatastore(config);
	} else if (implementation === 'DevelopmentDatastore'){
		ds = new DevelopmentDatastore();
	} else {
		throw new Error('Must specify implementation');
	}

	// async
	ds.create(function(err) {
		if (err === null)
			logger.info('** datastore connected, implementation is %s **', implementation);

		if (callback)
			callback(err, ds);
	});

	return ds;
};