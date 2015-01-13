'use strict';


// modules
var express = require('express'),
    kue = require('kue'),
	  request = require('request'),
    cors = require('cors');

var arcgisProcessor = require('./processors/arcgisProcessor');

var jobs = kue.createQueue();

jobs.process('arcgis',function(job,done){
  arcgisProcessor(job,done);
});


var app = express();

app.use(cors());
app.use(kue.app);
app.set('title', 'Columby Worker // kue');

var server = require('http').createServer(app);
server.listen(process.env.COLUMBY_WORKER_PORT || 7000, function(){
  console.log('Kue started.');
});
