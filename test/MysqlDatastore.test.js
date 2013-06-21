var createDatastore = require('../index').create;


var options = {
	implementation: 'MysqlDatastore',	
	database: 'mobilecore',
	host: 'localhost',
	user: 'mobilecore',
	password: 'MobileCore123123',
	port: 9999
};

createDatastore(options, function(err, datastore) {
	datastore.query('insert into test (t, testcol) values ("z", "y")', function(err, results) {
		console.log(arguments)
	})
})