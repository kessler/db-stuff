var $u = require('util')
var DatastoreBase = require('./DatastoreBase')
var pg = require('pg')

$u.inherits(PostgresDatastore, DatastoreBase)
function PostgresDatastore(config) {
	DatastoreBase.call(this, config)
	this.pg = pg
}

PostgresDatastore.prototype.create = function(callback) {
	var Client = pg.Client
	var Pool = pg.Pool

	if (this.config.useNative) {
		var native = require('pg').native
		Client = native.Client
		Pool = native.Pool
	}

	this.config.Client = Client

	this._pool = new Pool({
		connectionString: this.config.connectionString,
		log: this.config.log,
		max: this.config.poolSize || 10,
		Client: Client
	})
	
	var self = this
	this._connect(onConnect)

	function onConnect(err, client, done) {
		if (err === null) {
			client.query('select 1=1', onQuery)
		} else {
			callback(err)
		}

		function onQuery(err1, results) {
			done()
			callback(err1, self)
		}
	}
}

/*
	run the query and execute callback with results
*/
PostgresDatastore.prototype.query = function(sql, values, callback) {

	if (typeof(values) === 'function') {
		callback = values
		values = undefined
	}

	if (typeof(callback) !== 'function') {
		throw new Error('must provide a callback argument')
	}

	this._connect(onConnect)

	function onConnect(err, client, done) {
		if (err === null) {

			client.query(sql, values, function() {
				done()
				callback.apply(null, arguments)
			})

		} else {
			done()
			callback(err)
		}
	}
}

/*
	run the query and execute callback with a query instance
*/
PostgresDatastore.prototype.createQuery = function(sql, values, callback) {

	if (typeof(values) === 'function') {
		callback = values
		values = undefined
	}

	this._connect(onConnect)

	function onConnect(err, client, done) {
		if (err === null) {

			var query = client.query(sql, values)

			query.on('end', function() {
				done()
			})

			callback(null, query)
		} else {
			done(err)
			callback(err)
		}
	}
}

PostgresDatastore.prototype._connect = function(onConnect) {
	this._pool.connect(onConnect)
}

PostgresDatastore.prototype.end = function () {
	this.pg.end()
}

/*
	pg Dollar-Quoted String Constants - http://www.postgresql.org/docs/8.3/interactive/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING
*/
var randomTags = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'g', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'av', 'bv', 'cs', 'ds', 'es', 'fs', 'gd', 'hq', 'iy', 'gu', 'jn', 'kz', 'lz', 'ma', 'nf', 'op', 'pq', 'qg', 'rt', 'sl', 'tz']

function random(start, end) {
    var range = end - start
    return Math.floor((Math.random() * range) + start)
}

function createDollarQuotedString(val) {

	var randomTag = '$'+ randomTags[ random(0, randomTags.length) ] +'$'

	return randomTag + val + randomTag
}

PostgresDatastore.prototype.formatValue = function(val) {
	if (typeof(val) === 'undefined' || val === null) {
		return 'null'
	}

	if (typeof(val) === 'number') {
		return val
	}

	if ($u.isArray(val)) {
		return createDollarQuotedString(val.join(','))
	}

	return createDollarQuotedString(val)
}

module.exports = PostgresDatastore
