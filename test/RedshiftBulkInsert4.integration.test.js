var RedshiftBulkInsert = require('../lib/RedshiftBulkInsert4.js');
var async = require('async');
var path = require('path');
var s3shield = require('s3shield');
var assert = require('assert');
var clientProviderClass = s3shield.S3ClientProviderSelector.get('knox');
var clientProvider = new clientProviderClass();
var pg = require('pg');
var knox = require('knox');
var config = require('rc')('rsbi4');

var s3Client = knox.createClient({
    key: 		config.aws.accessKeyId,
  	secret: 	config.aws.secretAccessKey,
  	bucket: 	config.aws.bucket,
  	region: 	config.aws.region,
  	endpoint: 	config.aws.endpoint
});

var optionsMock = config.redshiftOptions;

var rowMock  = [1000, '', '127.0.0.1', 'mockadid', 'mocksubid', 'mockaction'];
var rowMock2 = [2000, '', '127.0.0.2', 'mock2adid', 'mock2subid', 'mock2action'];

var testFile = '1000||127.0.0.1|mockadid|mocksubid|mockaction\n2000||127.0.0.2|mock2adid|mock2subid|mock2action\n';

describe('RedshiftBulkInsert4', function(){
	it('should upload file to s3 and load data to datastore', function(done){
		this.timeout(180000);

		function testDone(err, services) {
			if (err) {
				console.error(err);
				done(err);
			} else {
				done(null);
			}
		}

		async.waterfall([
			initTest,
			truncateTempTable,
			doBulkInsert4,
			verifyUpload,
			verifyInsertedData
		], testDone);
	});
});

function verifyS3file(services, callback) {
	assert.strictEqual(services.downloadedFile, testFile);
}

function verifyInsertedData(services, callback) {
	services.ds.query(config.redshiftOptions.selectQuery, function(err, result) {
		if (err) {
			callback(err);
			return;
		}
		assert.equal(result.rows[1]['ip'], '127.0.0.2');
		assert.equal(result.rows[0]['ip'], '127.0.0.1');
		callback(null);
	});
}

function initTest(callback) {
	var services = {};
	var ds = require('../index.js').create(config.redshift, function(){
		services.ds = ds;
		callback(null, services);
	});
}

function truncateTempTable(services, callback) {
	console.log('Truncating temp table...');
	services.ds.query(config.redshiftOptions.truncateQuery, function(err, result){
		callback(err, services);
	});
}

function doBulkInsert4(services, callback) {
	console.log('doing bulk insert 4...');

	var rsbl = new RedshiftBulkInsert(services.ds, clientProvider, optionsMock, config.aws);
	services.rsbl = rsbl;

	rsbl.on('flush', function(flushOp){
		console.log('Flushing...')
		flushOp.on('success', function(flushOp){
			console.log('Flushed.');
			services.flushOp = flushOp;
			callback(null, services);
		});
		flushOp.on('error', function(err){
			console.error(err);
			callback(err, services);
		});
	});

	rsbl.insert(rowMock);
	rsbl.insert(rowMock2);
}

/*
	download the file from s3 and compare to test file data
*/
function verifyUpload(services, callback) {
	console.log('Verifying upload...');
	s3Client.getFile(services.flushOp.key, function(err, res) {

		services.downloadedFile = '';

		function readResponse() {

			if (!res) return;

			function readMore() {
				var result = res.read();

				if (result) {
					services.downloadedFile += result;
					readMore();
				}
			}

			res.on('readable', readMore);

			res.on('end', function () {
				callback(null, services);
			});
		}

		readResponse();
	});
}