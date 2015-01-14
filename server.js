'use strict';


// modules
var express = require('express'),
    kue = require('kue'),
	  request = require('request'),
    cors = require('cors');

var config = require('./config/settings');
var auth = require('./middleware/auth');

var arcgisProcessor = require('./processors/arcgisProcessor');
var fortesProcessor = require('./processors/fortesProcessor');

var app = express();


/**
 * Basic authentication for UI
 */
app.all('/*', auth.validateToken);


/**
 * Middleware
 */
app.use(cors());


/**
 * Initiate Kue
 */
var jobs = kue.createQueue();

jobs.process('arcgis', function(job,done){ arcgisProcessor(job,done); });
jobs.process('fortes', function(job,done){ fortesProcessor(job,done); });

app.use(kue.app);
app.set('title', 'Columby Worker // kue');


// Start the server
var server = require('http').createServer(app);
server.listen(config.port, function(){
  console.log('Kue started on port ' + config.port);
});
