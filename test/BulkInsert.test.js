var $u = require('util');
var assert = require('assert');
var createDatastore = require('../index').create;
var BulkInsert = require('../lib/BulkInsert');

describe('BulkInsert - basic operations', function() {
	var datastore = createDatastore('DevelopmentDatastore');
	var topic = new BulkInsert(datastore, 'test', ['a', 'b']);

	it('can accept an array of values and format it', function() {
		topic.insert([1, '2']);

		assert.strictEqual(topic.buffer.length, 1);
		assert.strictEqual(topic.buffer[0], '(1,\'2\')');
	});

	it('inserting an array row with field count that doesnt match the predefined field count will result in an error', function() {
		try {
			topic.insert([1,'2',3]);
			assert.fail('should have thrown an error because number of fields in inserted row is not 2');
		} catch(e) {
			assert.ok('expected error was thrown');
		}
	});

	it('generate a multi line insert query when flushing, made from all the values in the buffer and then clear the buffer', function() {			
		topic.flush();			
		assert.strictEqual(datastore.queries.length, 1);						
		assert.strictEqual(datastore.queries[0], "insert into test (a,b) values (1,'2')");
		assert.strictEqual(topic.buffer.length, 0);
	});

	it('flushes when a threshold is reached', function() {
		// insert threshold -1 rows
		for (var i = 0; i < topic.params.threshold - 1; i++) 
			topic.insert([1, '2']);

		// make sure everything is as expected
		assert.strictEqual(topic.buffer.length, topic.params.threshold - 1);

		topic.insert([1, '2']);
		assert.strictEqual(topic.buffer.length, 0);			
	});

});

describe('BulkInsert - construction', function() {
	var datastore = createDatastore('DevelopmentDatastore');
	var topic =	new BulkInsert(datastore, 'test', { threshold: 10 }, ['a', 'b', 'c']);

	it('sets the field count accordingly', function() {
		assert.strictEqual(topic.fieldCount, 3);
	});

	it('creates a template for the insert query', function() {			
		assert.strictEqual(topic.sqlBase, 'insert into test (a,b,c) values ');
	});

	it('throws an error if the number of fields in a row does not match the field count', function() {
		try {
			topic.insert([1,3]);
			assert.fail('should have thrown an error because number of fields in inserted row is not 2');
		} catch(e) {
			assert.ok('expected error was thrown');
		}
	});

	it('when a flush occurs field count will not reset', function() {
		for (var i = 0; i < topic.params.threshold; i++) {
			topic.insert([1,2,3]);
		}

		assert.strictEqual(datastore.queries.length, 1);
		assert.strictEqual(topic.buffer.length, 0);
		assert.strictEqual(topic.fieldCount, 3);
	});
});

describe('BulkInsert - idle flushing', function () {
	it('flushes after idling for a certain amount of time', function(done) {
		var datastore = createDatastore('DevelopmentDatastore');
		var topic = new BulkInsert(datastore, 'test', { idleFlushPeriod: 100 }, ['a']);

		topic.insert([1]);

		var self = this;

		setTimeout(function() {		
			assert.strictEqual(datastore.queries.length, 1);
			assert.strictEqual(topic.buffer.length, 0);
			done();
		}, 110);	
	});

	it('resets the idle period if a flush occurs due to other reasons', function (done) {
		var datastore = createDatastore('DevelopmentDatastore');
		var topic = new BulkInsert(datastore, 'test', { threshold: 10, idleFlushPeriod: 100 }, ['a']);
		topic.insert([1]);
		var self = this;

		setTimeout(function() {
			for (var i = 0; i < 10; i++)
				topic.insert([1]);

			//check first flush occurred due to threshold
			assert.strictEqual(topic.buffer.length, 1);
			assert.strictEqual(datastore.queries.length, 1);			

			//flush period should have reset to 100ms
			setTimeout(function() {
				//no flush should have happened by now
				assert.strictEqual(topic.buffer.length, 1);
				assert.strictEqual(datastore.queries.length, 1);			

				//after additional 50ms an idle flush should occur
				setTimeout(function() {
					assert.strictEqual(topic.buffer.length, 0);
					assert.strictEqual(datastore.queries.length, 2);			
					done();
				}, 51);

			}, 50);

		}, 50);
	})
});

describe('BulkInsert is an event emitter', function() {
	var datastore = createDatastore('DevelopmentDatastore');
	var topic = new BulkInsert(datastore, 'test', { threshold: 10, idleFlushPeriod: 100 }, ['a']);
	
	it('fire an event when a flush occurs', function (done) {
		
		topic.on('flush', callback);

		for (var i = 0; i < topic.params.threshold; i++)
			topic.insert([1]);

		function callback(err, results, sql) {								
			assert.strictEqual(sql, 'insert into test (a) values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)');
			//TODO: need a better way to make sure this is infact some sort of query, maybe an interface or something, exported from Datastore
			done();
		}
	});
});
	

// describe('BulkInsert memory footprint', function() {

// });	
		

