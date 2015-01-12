'use strict';


// modules
var kue = require('kue'),
	  request = require('request'),
    cors = require('cors');

var arcgisProcessor = require('./processors/arcgisProcessor');


var jobs = kue.createQueue();

jobs.process('arcgis',function(job,done){
  arcgisProcessor(job,done);
});


kue.app.set('title', 'Columby Worker // kue');
kue.app.use(cors());

kue.app.listen(process.env.COLUMBY_WORKER_PORT || 7000);
console.log('kue started... ');
