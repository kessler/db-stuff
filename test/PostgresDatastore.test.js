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

			instance.query('select 1+1 as result', function(err, result) {
				if (err) return done(err)
					
				assert(result.rows.length === 1)
				assert(result.rows[0].result === 2)
				instance.end()
				done()
			})
		})
	})

	it('end connection', function (done) {
		createDatastore(config, function (err, instance) {
			if (err) return done(err)

			instance.end()
			done()
		})
	})
})
