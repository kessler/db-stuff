var vows = require('vows');
var assert = require('assert');

var Insert = require('../lib/Datastore.Insert');
var Datastore = require('../lib/Datastore');

var suite = vows.describe('Insert');

suite.addBatch({
	'encapsulates an insert operation to a specific datastore and table, ': {
		
		'it can then be reused to insert multiple rows': {
			topic: function() {
				try {
					this.datastore = Datastore.create('DevelopmentDatastore');
					this.command = new Insert(this.datastore, 'test', [ 'x', 'y' ]);
					this.command.execute([ '1', '2' ], this.callback);
					this.command.execute([ '5', '6' ], this.callback);
				} catch (e) {
					console.log(e);
				}
			},
			'callback': function(err, results) {
				assert.isNull(err);	
				assert.lengthOf(this.datastore.queries, 2);
				assert.includes(this.datastore.queries, 'insert into test (x,y) values (\'1\',\'2\')');				
				assert.includes(this.datastore.queries, 'insert into test (x,y) values (\'5\',\'6\')');				
			}
		}
	}
});

suite.options.error = false;

suite.export(module);
