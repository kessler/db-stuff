var $u = require('util')
var DatastoreBase = require('./DatastoreBase')
var mysql = require('mysql')

$u.inherits(MysqlDatastore, DatastoreBase)
function MysqlDatastore(config) {
	DatastoreBase.call(this, config)
}

MysqlDatastore.prototype.create = function(callback) {
	this.pool = mysql.createPool(this.config)

	var self = this
	this.createQuery('select 1=1', function(err, query, connection) {			
		if (err)
			return callback(err)

		self.timezone = connection.config.timezone

		query.on('end', function () {			
			callback(null, self)			
		})

		query.on('error', function (err) {			
			callback(err)			
		})
	})
}

/*
	run the query and execute callback with results
*/
MysqlDatastore.prototype.query = function(sql, values, callback) {	
	
	if (typeof(values) === 'function') {
		callback = values
		values = undefined
	}

	if (typeof(callback) !== 'function') {
		throw new Error('must provide a callback argument')
	}

	this.pool.getConnection(function(err, connection) {
		if (err) {
			if (connection)
				connection.destroy()
			
			if (callback)
				callback(err)

			return
		}
		
		connection.query(sql, values, function(err, rows) {
			if (err)
				connection.destroy()
			else
				connection.end()
			
			if (callback)
				callback(err, rows)
		})
	})
}

/*
	run the query and execute callback with a query instance
*/
MysqlDatastore.prototype.createQuery = function(sql, values, callback) {
	
	if (typeof(values) === 'function') {
		callback = values
		values = undefined
	}


	this.pool.getConnection(function(err, connection) {
		if (err) {
			if (connection)
				connection.destroy()

			if (callback)
				callback(err)

			return
		}

		var query = connection.query(sql, values)

		query.on('error', function(err) {
			connection.destroy()
		})

		query.on('end', function() {
			connection.end()
		})
	
		// query.on('fields', function(fields) {
		// 	// the field packets for the rows to follow
		// })

		// query.on('result', function(row) {
		// 	// Pausing the connnection is useful if your processing involves I/O
		// 	connection.pause()

		// 	processRow(row, function() {
		// 		connection.resume()
		// 	})
		// })
		if (callback)
			callback(null, query, connection)
	})	
}


MysqlDatastore.prototype.formatValue = function(val) {
	return mysql.SqlString.escape(val, false, this.timezone)
}

module.exports = MysqlDatastore
