var logger = require('log4js').getLogger('db-stuff');

var PostgresDatastore = module.exports.PostgresDatastore = require('./lib/PostgresDatastore');
var MysqlDatastore = module.exports.MysqlDatastore = require('./lib/MysqlDatastore');
var DevelopmentDatastore = module.exports.DevelopmentDatastore = require('./lib/DevelopmentDatastore');
var DatastoreBase = module.exports.DatastoreBase = require('./lib/DatastoreBase');

//backward compatibility:
module.exports.Datastore = {
	PostgresDatastore: PostgresDatastore,
	DevelopmentDatastore: DevelopmentDatastore,
	MysqlDatastore: MysqlDatastore,
	DatastoreBase: DatastoreBase,
	create: create
};
//end backward compatibility:

module.exports.Insert = require('./lib/Insert.js');
module.exports.BulkInsert = require('./lib/BulkInsert.js');
module.exports.RedshiftBulkInsert = require('./lib/RedshiftBulkInsert2.js');

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
		ds = new PostgresDatastore(config);
	} else if (implementation === 'MysqlDatastore') {
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
			logger.info('** datastore connected, implementation is %s **', implementation);

		if (callback) {
			if (err)
				err.implementation = implementation;
			callback(err, ds);
		}
	});

	return ds;
};

module.exports.create = create;


