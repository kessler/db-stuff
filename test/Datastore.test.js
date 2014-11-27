var assert = require('assert')
var dbStuff = require('../index')
var createDatastore = dbStuff.create

describe('Datastore', function () {
	var db

	it('Creates instances using an async factory method', function (done) {
		assert.ok(typeof(db) !== 'undefined')
		assert.ok(db instanceof dbStuff.DevelopmentDatastore)
		done()		
	})

	it('format field', function () {
		assert.strictEqual(db.formatField('asd!!!123'), 'asd___123')
	})

	beforeEach(function (done) {
		createDatastore("DevelopmentDatastore", function (err, instance) {
			db = instance
			done(err)
		})
	})
})
