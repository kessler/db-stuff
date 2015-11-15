var expect = require('chai').expect;
var BulkInsert = require('../lib/BulkInsert');
var dbStuff = require('../index');

// TODO add error code paths tests

describe('BulkInsert', function () {
	var insert, db;
	it('creates a bulk insert query from an array of row data and executes it', function (done) {
		insert.execute([
			[1, 2, 3],
			[4, 5, 6]
		], function (err) {
			expect(err).to.be.null;
			expect(db.queries).to.have.length(1);
			expect(db.queries).to.contain('insert into test (a,b,c) values  (1,2,3),\n(4,5,6)');
			done();
		});
	});

	beforeEach(function () {
		db = dbStuff.create('DevelopmentDatastore');
		insert = new BulkInsert(db, 'test', ['a', 'b', 'c']);
	})
})
