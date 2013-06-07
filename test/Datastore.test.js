var assert = require('assert');
var Datastore = require('../lib/Datastore');

describe('Datastore', function () {
	it('Creates instances using an async factory method', function (done) {
		Datastore.create("DevelopmentDatastore", function (err, instance) {
			if (err)
				assert.fail(err);

			assert.ok(typeof(instance) !== 'undefined');
			assert.ok(instance instanceof Datastore.DevelopmentDatastore);
			done();
		});	
	});
});
