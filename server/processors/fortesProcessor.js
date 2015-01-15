'use strict';

var request = require('request');
var fortesWorker = require('../workers/fortesWorker');

/**
 *
 * Process a new Fortes Job
 *
 * @param job
 * @param done
 *
 */
module.exports = function(job,done){
  console.log('Processing Fortes');
  var ID = job.data.ID;

  var options={
    url: 'http://188.166.31.214:8000/api/v2/dataset/'+ID,
    timeout: 1000
  };

  request(options, function(error,response,body) {

    if(error) done('Error getting dataset (id: '+ID+') info');

    if(!error && response.statusCode == 200){
      var data = JSON.parse(body);

      if (data && data.primary) {
        // start fortes scraper
        fortesWorker(job, data, done);
      } else {
        // Todo: Update metadata from 'queued' to 'error'. Needs general pg_metadata connection?

        done('Error getting dataset (id: ' + ID + ') info');
      }
    }
  });
};
