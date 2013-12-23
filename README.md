TODO: complete transformation from vows to mocha

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

	datastore.query('select * table where x=%', [1], function(err, data) {
		console.log(err, data);
	});

	datastore.createQuery('select * from table where x=%', [1], function(err, q) {
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


	//reusable insert command
	function cb(err) {
		console.log(err);
	}

	var insertCommand = datastore.insert('table', ['fieldA', 'fieldB']);

	insertCommand.insert([1, 2], cb);

	insertCommand.insert('1,2', cb);
});


```