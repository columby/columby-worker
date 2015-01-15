'use strict';

// modules
var express = require('express'),
    kue = require('kue'),
	  request = require('request'),
    cors = require('cors'),
    basicAuth = require('basic-auth-connect'),
    path = require('path');

var config = require('./../config/settings');
var auth = require('./middleware/auth');

var arcgisProcessor = require('./processors/arcgisProcessor');
var fortesProcessor = require('./processors/fortesProcessor');

var app = express();

// Allow cors
app.use(cors());

// App settings
app.engine('html', require('ejs').renderFile);
app.set('view engine', 'html');
app.use(express.static(path.join(config.root, 'public')));
app.set('appPath', config.root + '/public');

// JWT authentication for jobs
app.post('/job/*', auth.validateToken);
app.delete('/job/*', auth.validateToken);
app.get('/app', auth.basic, function(req,res){
  res.sendFile(app.get('appPath') + '/index.html');
});

// Initiate Kue
var jobs = kue.createQueue();

// Job Handlers
jobs.process('arcgis', function(job,done){ arcgisProcessor(job,done); });
jobs.process('fortes', function(job,done){ fortesProcessor(job,done); });

app.use(kue.app);
app.set('title', 'Columby Worker // kue');


// Start the server
var server = require('http').createServer(app);
server.listen(config.port, function(){
  console.log('Kue started on port ' + config.port);
});
