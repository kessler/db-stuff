var RedshiftBulkInsert = require('../lib/RedshiftBulkInsert4.js');
var path = require('path');
var LocalFSClient = require('s3shield').LocalFSClient;

var clientProvider = new LocalFSClient.Provider();

var ds = require('../index.js').create('DevelopmentDatastore');

//var rsbl = new RedshiftBulkInsert(ds, clientProvider, {}, {});

/*
	integration test should include:

	1. full cycle

*/
