var RedshiftBulkInsert = require('../lib/RedshiftBulkInsert2.js');
var path = require('path');

function MockDatastore() {
	var queries = this.queries = [];
	this.query = function(sql, callback) {
		queries.push(sql);
		callback(null);
	}
}

var testroot = path.join(__dirname, 'testfiles');
var logfile = path.join(testroot, 'testlog');
var testRow = [1,2,3,'root', null];
var options = {
	path: testroot,
	tableName: 'liga',
	threshold: 1000,
	idleFlushPeriod: 100000,
	fields: ['a', 'b', 'c', 'd', 'e']
};

var awsOptions = { 
	region: '1', 
	accessKeyId: '2', 
	secretAccessKey: '3',
	bucket: '4'
};

describe('stress test', function () {
	it('', function (done) {

		this.timeout(50000);
		
		var rsbl = new RedshiftBulkInsert(new MockDatastore(), options, awsOptions);

		rsbl.on('flush', function () {
			console.log('flush');
		});

		var count = 0;

		function batch() {	

			if (count++ > 10) return done();

			for (var i = 0; i < 1000; i++) {
				rsbl.insert(testRow);
			}

			setTimeout(batch, 2000);
		}

		var ref = setInterval(function () {
			console.log(rsbl._inserts);
		}, 1000);

		ref.unref();

		batch();

	})	
})

