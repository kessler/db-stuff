var vows = require('vows');
var assert = require('assert');
var Datastore = require('../lib/Datastore');
var BulkInsert = require('../lib/BulkInsert');

var suite = vows.describe('BulkInsert');

var batch1dsA = Datastore.create('DevelopmentDatastore');
var batch1dsB = Datastore.create('DevelopmentDatastore');

var batch2dsA = Datastore.create('DevelopmentDatastore');
var batch2dsB = Datastore.create('DevelopmentDatastore');

var batch3dsA = Datastore.create('DevelopmentDatastore');

suite.addBatch({
	'basic operations': {
		topic: function() {
			var bi = new BulkInsert(batch1dsA, 'test', ['a', 'b']);

			return bi;
		},
		'can insert an array and it will be formatted': function(topic) {
			topic.insert([1, '2']);

			assert.lengthOf(topic.buffer, 1);
			assert.strictEqual(topic.buffer[0], '(1,\'2\')');
		},		
		'inserting an array row with field count that doesnt match the field count will result in an error': function(topic) {
			try {
				topic.insert([1,'2',3]);
				assert.fail('should have thrown an error because number of fields in inserted row is not 2');
			} catch(e) {
				assert.ok('expected error was thrown');
			}
		},
		'when flushing, insert a query to datastore made from all the values in the buffer and clear the buffer': function(topic) {			
			topic.flush();			
			assert.lengthOf(batch1dsA.queries, 1);						
			assert.strictEqual(batch1dsA.queries[0], "insert into test (a,b) values (1,'2')");
			assert.lengthOf(topic.buffer, 0);
		},
		'when threshold is reached flush occurs': function(topic) {

			// insert threshold -1 rows
			for (var i = 0; i < topic.params.threshold - 1; i++) 
				topic.insert([1, '2']);

			// make sure everything is as expected
			assert.lengthOf(topic.buffer, topic.params.threshold - 1);

			topic.insert([1, '2']);
			assert.lengthOf(topic.buffer, 0);			
		}		
	},
	'when fields are specified in the construction of the bulk insert': {
		topic: function() {
			return new BulkInsert(batch1dsB, 'test', { threshold: 10 }, ['a', 'b', 'c']);
		},
		'field count is wired in before a single insert occurs': function(topic) {
			assert.strictEqual(topic.fieldCount, 3);
		},
		'fields are formatted into the sql base used in each insert': function(topic) {			
			assert.strictEqual(topic.sqlBase, 'insert into test (a,b,c) values ');
		},
		'an error is thrown if the number of fields in a row does not match the field count': function(topic) {
			try {
				topic.insert([1,3]);
				assert.fail('should have thrown an error because number of fields in inserted row is not 2');
			} catch(e) {
				assert.ok('expected error was thrown');
			}
		},
		'when a flush occurs field count will not reset': function(topic) {
			for (var i = 0; i < topic.params.threshold; i++) {
				topic.insert([1,2,3]);
			}

			assert.lengthOf(batch1dsB.queries, 1);
			assert.lengthOf(topic.buffer, 0);
			assert.strictEqual(topic.fieldCount, 3);
		}
	}
});

suite.addBatch({
	'flush occurs after idling for a certain amount of time': {
		topic: function() {
			var bi = new BulkInsert(batch2dsA, 'test', { idleFlushPeriod: 100 }, ['a']);
			bi.insert([1]);
			var self = this;
			setTimeout(function() {
				self.callback(null, bi);
			}, 102);
		},
		'check flush occurred': function (bi) {
			assert.lengthOf(batch2dsA.queries, 1);
			assert.lengthOf(bi.buffer, 0);
		}
	},
	'idle period is reset if a flush occurs earlier': {
		topic: function() {
			var bi = new BulkInsert(batch2dsB, 'test', { threshold: 10, idleFlushPeriod: 100 }, ['a']);
			bi.insert([1]);
			var self = this;

			setTimeout(function() {
				for (var i = 0; i < 10; i++)
					bi.insert([1]);

				self.callback(null, bi);
			}, 50);
		},
		'check first flush occurred due to threshold': function(bi) {
			assert.lengthOf(bi.buffer, 1);
			assert.lengthOf(batch2dsB.queries, 1);			
		},
		'flush should be 100ms after threshold flush occurred - first check after 50ms to make sure it was delayed from the original eta': {
			topic: function(bi) {
				var self = this;
				setTimeout(function() {
					self.callback(null, bi);
				}, 50);
			},
			'no flush should have happened by now': function(bi) {
				assert.lengthOf(bi.buffer, 1);
				assert.lengthOf(batch2dsB.queries, 1);
			},
			'after additional 50ms an idle flush should occur': {
				topic: function(bi) {
					var self = this;
					setTimeout(function() {
						self.callback(null, bi);
					}, 51);
				},
				'check idle flush did occur': function(bi) {
					assert.lengthOf(bi.buffer, 0);
					assert.lengthOf(batch2dsB.queries, 2);
				}
			}
		}
	}
});

suite.addBatch({
	'BulkInsert is an event emitter': {
		topic: function() {
			return new BulkInsert(batch3dsA, 'test', { threshold: 10, idleFlushPeriod: 100 }, ['a']);
		},
		'when a flush occurs it fires an event': {
			topic: function(topic) {				
				topic.on('flush', this.callback);
				for (var i = 0; i < topic.params.threshold; i++)
					topic.insert([1]);
			},
			'event callback': function(err, results, sql) {								
				assert.strictEqual(sql, 'insert into test (a) values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)');
				//TODO: need a better way to make sure this is infact some sort of query, maybe an interface or something, exported from Datastore
			}
		}
	}
})

suite.export(module);