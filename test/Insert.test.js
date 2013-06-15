var assert = require('assert');

var Insert = require('../lib/Insert');
var Datastore = require('../index');

describe('Insert encapsulates an insert operation to a specific datastore and table, ', function () {
	it('it can then be reused to insert multiple rows', function(done) {
		
		var datastore = Datastore.create('DevelopmentDatastore');
		var command = new Insert(datastore, 'test', [ 'x', 'y' ]);
		command.execute([ '1', '2' ], function() {				
			command.execute([ '5', '6' ], callback);
		});
		
		function callback(err, results) {
			assert.strictEqual(err, null);				
			assert.strictEqual(datastore.queries.length, 2);
			assert(datastore.queries.indexOf('insert into test (x,y) values (\'1\',\'2\')') > -1);				
			assert(datastore.queries.indexOf('insert into test (x,y) values (\'5\',\'6\')') > -1);				
			
			done();
		}		
	});
});

