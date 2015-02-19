'use strict';

var pg = require('pg'),
    settings = require('../config/environment'),
    csvWorker = require('../workers/csv.worker'),
    arcgisWorker = require('../workers/arcgis.worker'),
    fortesWorker = require('../workers/fortes.worker');


var Worker = module.exports = function(config, callback) {
  var self=this;

  self._config        = config ? config : {};
  self._processEvery = config.processEvery || '5000';
  self._processing   = false;
  self._job          = {};
  self._connection   = {};

  // connect to the cms database
  pg.connect(settings.db.cms.uri, function(err,client,done){
    self._connection = {
      client: client,
      done: done
    };
    callback(err);
  });
};

/**
 *
 * Start the worker
 *
 */
Worker.prototype.start = function() {
  var self=this;

  /**
   *
   * Init
   *
   */
  clearProcessingList(function(){
    console.log('Clean list finished.');
    console.log('Setting process interval: ' + self._processEvery/1000 + ' sec');
    self._processInterval = setInterval(processJob.bind(self), self._processEvery);
    process.nextTick(processJob.bind(self));
    console.log('Worker started. ');
  });


  /**
   * Worker was (re)started. Set possible strained jobs to error.
   * @param cb
   */
  function clearProcessingList(cb){
    // Clear jobs in progress and set them to error state.
    console.log('Clearing list');
    var sql='UPDATE "Jobs" SET status=$1, error=$2 WHERE status=$3';
    var values = ['error','Processor restarted','processing'];
    self._connection.client.query({text:sql,values:values}, function(err,result){
      if (err) { console.log('err',err); return cb(err); }
      if (!result.rows.count || result.rows.count<1){ return cb('No job found.'); }
      cb(result);
    })
  }


  /**
   *
   * Check for a job and process it
   *
   */
  function processJob() {
    var self=this;

    if (self._processing){
      // return if already processing
      console.log('Already processing. ');
      return;
    }

    // turn on processing flag
    self._processing = true;

    // select the next job in queue
    var sql =
      'SELECT ' +
        '* ' +
      'FROM ' +
        '"Jobs" ' +
      'WHERE "Jobs"."status"=\'active\' ' +
      'ORDER BY "created_at" DESC ' +
      'LIMIT 1';
    self._connection.client.query(sql, function(err, result){

      // return and turn off processing flag if error.
      if (err) {
        console.log('Error connecting to the cms-client', err);
        return self._processing=false;
      }

      // Set the current job
      self._job = result.rows[ 0];

      // Return if no job found
      if (!self._job){
        self._processing=false;
        return;
      }

      // Determine job type and process it
      var sql;
      switch(self._job.type){
        case 'csv':
          // get file url
          sql=
            'SELECT ' +
              '"Primaries".id AS "primaryId", ' +
              '"Distributions".id AS "distributionId", '+
              '"Files".id AS "fileId", ' +
              '"Files".url AS "fileUrl" ' +
            'FROM "Primaries" ' +
              'LEFT JOIN "Distributions" ' +
                'ON "Primaries"."distribution_id"="Distributions"."id" ' +
              'LEFT JOIN "Files"' +
                'ON "Files".id="Distributions"."file_id"' +
            'WHERE "Primaries".dataset_id=' + self._job.dataset_id;
          self._connection.client.query(sql, function(err,result){
            // Handle error
            if (err){return handleProcessedJob(err,null);}

            if ( result.rows[0]) {
              self._job.data = result.rows[0];
              var csv = new csvWorker();
              // Start processing
              csv.start(self._job, handleProcessedJob);
            } else {
              handleProcessedJob('No valid record found. ', null);
            }
          });
          break;

        case 'arcgis':
          // get url
          sql=
            'SELECT ' +
              '"Primaries".id AS "primaryId", ' +
              '"Distributions".id AS "distributionId", ' +
              '"Distributions"."accessUrl" AS "url" '+
            'FROM "Primaries" ' +
              'LEFT JOIN "Distributions" ' +
                'ON "Primaries"."distribution_id"="Distributions"."id" ' +
            'WHERE "Primaries".dataset_id=' + self._job.dataset_id;
          console.log('sql', sql);
          self._connection.client.query(sql, function(err,result){
            if (err){ return handleProcessedJob(err,null); }
            if (!result.rows[ 0]){ return handleProcessedJob('No valid record found. ',null); }
            self._job.data = result.rows[ 0];
            var arcgis = new arcgisWorker();
            arcgis.start(self._job, handleProcessedJob);
          });
          break;

        case 'fortes':
          sql=
            'SELECT ' +
              '"Primaries".id AS "primaryId", ' +
              '"Distributions".id AS "distributionId" '+
            'FROM "Primaries" ' +
              'LEFT JOIN "Distributions" ' +
                'ON "Primaries"."distribution_id"="Distributions"."id" ' +
            'WHERE "Primaries".dataset_id=' + self._job.dataset_id;
          self._connection.client.query(sql, function(err,result){
            // Handle error
            if (err){return handleProcessedJob(err, null);}
            if ( result.rows[0]) {
              self._job.data = result.rows[0];
              var fortes = new fortesWorker();
              // Start processing
              fortes.start(self._job, handleProcessedJob);
            } else {
              handleProcessedJob('No valid record found. ', null);
            }
          });
          break;

        default:
          handleProcessedJob('Job type '+ self._job.type +' not found for job id: ' + self._job.id);
          break;
      }
    });
  }


  /**
   *
   * Handler for when a job is finished processing.
   *
   */
  function handleProcessedJob(err){

    console.log('handling processed job');
    console.log(self._job.data);

    if (err) {
      // update Job status in CMS
      var sql = 'UPDATE "Jobs" SET "status"=\'error\', "error"=\''+err+'\' WHERE id=' + self._job.id;
      self._connection.client.query(sql);
      console.log('There was an error', err);
    } else {
      // create downloadable file

    }

    self._processing = false;
    console.log('Processing done for Job: ' + self._job.id);
    console.log('=================================');
  }

};
