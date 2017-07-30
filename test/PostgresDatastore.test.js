var assert = require('assert')
var dbStuff = require('../index')
var createDatastore = dbStuff.create
var PostgresDatastore = dbStuff.PostgresDatastore
var config = require('rc')('test', {})
config.implementation = 'PostgresDatastore'

describe('PostgresDatastore', function () {
	var db

	it('connects', function (done) {
		createDatastore(config, function (err, instance) {
			if (err) {
				return done(err)
			}

			assert(instance instanceof PostgresDatastore)
			instance.end()
			done()
		})
	})

	it('query', function (done) {
		createDatastore(config, function (err, instance) {
			if (err) return done(err)

			instance.query('select * from test', function(err, result) {
				if (err) return done(err)

				assert(result.rows.length === 1)
				assert(result.rows[0].foo === 1)
				assert(result.rows[0].bar === 'test')

				instance.end()
				done()
			})
		})
	})

	// it('format field', function () {
	// 	assert.strictEqual(db.formatField('asd!!!123'), 'asd___123')
	// })
})
