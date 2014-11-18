# Database abstraction
Attempt to provide a unified low level interface to various databases. Currently supports:

1. MySql
2. PostgreSql

one need to npm install pg / mysql in order to use those implementations

### Example
```
 $ npm install db-stuff pg
```

```
var dbStuff = require('db-stuff');

var config = {
	implementation: 'PostgresDatastore',
	connectionString: 'tcp://user:pass@redshift.host:5439/db'
};

dbStuff.create(config, function (err, datastore) {

	datastore.query('select * table', function(err, data) {
		console.log(err, data);
	});
	
	// with params
	datastore.query('select * table where x=?', [1], function(err, data) {
		console.log(err, data);
	});

	datastore.insert('table', { a: 1, b: 2}, function (err) {
	})

	datastore.update('table', { a: 1, b: 3}, function (err) {		
	})

	datastore.update('table', { a: 1, b: 3}, {id: 1, b:2 } function (err) {		
		// update record where id = 1 AND b = 2
		// this simple filter only supports AND(s)
		// for more complex stuff just run query() 
	})

	// with params
	datastore.createQuery('select * from table where x=?', [1], function(err, q) {
		q.on('row', function(row) {

		});

		q.on('error', function(err) {

		});

		q.on('end', function(results) {

		});
	});

	datastore.createQuery('select * from table', function(err, q) {
		...
		...
		...
	});


	//reusable/batch insert command
	function cb(err) {
		console.log(err);
	}

	var insertCommand = datastore.newInsertCommand('table', ['fieldA', 'fieldB']);

	insertCommand.execute([ [1, 2], [3, 4] ],, cb);
	insertCommand.execute([1,2], cb);

	// raw strings - will be places directly inside VALUES (...), this is very unsafe though
	insertCommand.execute('1,2', cb);
});


```