var assert = require('assert');
var dbStuff = require('../index');
var createDatastore = dbStuff.create;

describe('Datastore', function () {
	it('Creates instances using an async factory method', function (done) {
		createDatastore("DevelopmentDatastore", function (err, instance) {
			if (err)
				assert.fail(err);

			assert.ok(typeof(instance) !== 'undefined');
			assert.ok(instance instanceof dbStuff.DevelopmentDatastore);
			done();
		});	
	});
});
