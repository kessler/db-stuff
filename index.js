var debug = require('debug')('db-stuff');

var BlackholeDatastore = module.exports.BlackholeDatastore = require('./lib/BlackholeDatastore');
var PostgresDatastore
try {
	PostgresDatastore = module.exports.PostgresDatastore = require('./lib/PostgresDatastore');
} catch (ep) {
	if (ep.code !== 'MODULE_NOT_FOUND')
		throw ep
}

var MysqlDatastore
try {
	MysqlDatastore= module.exports.MysqlDatastore = require('./lib/MysqlDatastore');
} catch (em) {
	if (em.code !== 'MODULE_NOT_FOUND')
		throw em
}

var DevelopmentDatastore = module.exports.DevelopmentDatastore = require('./lib/DevelopmentDatastore');
var DatastoreBase = module.exports.DatastoreBase = require('./lib/DatastoreBase');

module.exports.Insert = require('./lib/Insert.js');

module.exports.create = create;

//backward compatibility:
module.exports.Datastore = {
	PostgresDatastore: PostgresDatastore,
	DevelopmentDatastore: DevelopmentDatastore,
	MysqlDatastore: MysqlDatastore,
	DatastoreBase: DatastoreBase,
	create: create
};
//end backward compatibility:



/*
	factory for creating datastores.

	usage:

	var ds = Datastore.create( { implementation: 'SomeImplementation', logEnabled: false, additional config params... }, myCallback);

	var ds = Datastore.create( { implementation: 'SomeImplementation', additional config params... });

	var ds = Datastore.create('SomeImplementation', myCallback);

	var ds = Datastore.create('SomeImplementation');
*/
function create(config, callback) {
	var ds;

	var implementation;
	var logEnabled = true;

	if (typeof(config) === 'string') {
		implementation = config;
	} else {
		implementation = config.implementation;

		if (config.logEnabled !== undefined)
			logEnabled = config.logEnabled;
	}

	if (implementation === 'PostgresDatastore') {
		if (!PostgresDatastore)
			throw new Error('try npm install pg first');

		ds = new PostgresDatastore(config);
	} else if (implementation === 'MysqlDatastore') {
		if (!MysqlDatastore)
			throw new Error('try npm install mysql first');

		ds = new MysqlDatastore(config);
	} else if (implementation === 'DevelopmentDatastore'){
		ds = new DevelopmentDatastore();
	} else if (implementation === 'BlackholeDatastore'){
		ds = new BlackholeDatastore();
	} else {
		throw new Error('Must specify implementation');
	}

	// async
	ds.create(function(err) {
		if (err === null && logEnabled)
			debug('** datastore connected, implementation is %s **', implementation);

		if (callback) {
			if (err)
				err.implementation = implementation;
			callback(err, ds);
		}
	});

	return ds;
};