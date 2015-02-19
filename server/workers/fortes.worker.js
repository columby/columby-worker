'use strict';

var request = require('request'),
  pg = require('pg'),
  escape = require('pg-escape'),
  //copyFrom = require('pg-copy-streams').from,
  config = require('../config/environment');


var FortesWorker = module.exports = function() {
  var self=this;

  self._tablename = null;
  self._columns = [];
  self._batch = [];
  self._batchInProgress = false;
  self._batchSize = 50;
  self._batchedPaused = false;
  self._tableCreated = false;
};


FortesWorker.prototype.start = function(job,callback) {

  var self = this;
  self._job = job;

  /**
   *
   * Init
   *
   */
  console.log('Starting fortes worker.');
  connect(function(err){
    if (err) {
      handleError('There was an error connecting to the DBs.');
      return callback(err)
    }
    // validate job data
    validateData(function(err) {
      if (err) {
        handleError('There was an error validating the data.');
        return callback(err)
      }
      process(function (err) {
        if (err) {
          console.log('There was an error processing the data.', err);
          handleError('There was an error processing the data.');
          return callback(err)
        }
        // finish
        finish(function (err) {
          if (err) {
            console.log('There was an error finishing.', err);
            handleError('There was an error finishing.');
            return callback(err)
          }
          // complete
          callback(err);
        });
      });
    });
  });


  /**
   * Connect to CMS and GEO db
   *
   * @param callback
   */
  function connect(callback) {
    self._connection = {};
    // Connect to cms
    pg.connect(config.db.cms.uri, function (err, client, done) {
      if (err) { return callback(err); }
      console.log('Connected to CMS DB');
      self._connection.cms = {
        client: client,
        done: done
      };
      // Connect to postgis
      pg.connect(config.db.postgis.uri, function (err, client, done) {
        if (err) { return callback(err); }
        console.log('Connected to Data DB.');
        self._connection.data = {
          client: client,
          done: done
        };
        callback();
      });
    });
  }


  /**
   *
   * Validate if required elements in job are present.
   *
   * @param callback
   * @returns {*}
   */
  function validateData(callback){

    if (!self._job.data.primaryId) {
      return callback('No primary ID!');
    }

    self._tablename = 'primary_' + self._job.data.primaryId;
    console.log('Tablename is ' + self._tablename);

    // Delete columby data-table
    self._connection.data.client.query('DROP TABLE IF EXISTS ' + self._tablename, function(err, result) {
      if(err) {
        return callback('Error create table if not exist.');
      }
      console.log('Existing table dropped. ');
      callback();
    });
  }


  function process(callback) {
    request.get(config.fortes.url, {
      'auth': {
        'user': config.fortes.username,
        'pass': config.fortes.password,
        'sendImmediately': false
      }
    },function(err, res, body){
      if (err) {
        return callback(err);
      }
      var data = JSON.parse(body);
      // transform into array
      var dataArray = [];
      for( var i in data ) {
        if (data.hasOwnProperty(i)){
          dataArray.push(data[i]);
        }
      }
      //console.log(dataArray[ 0]);

      var result = [];

      // create columns
      self._columns = Object.keys(dataArray[ 0]);
      console.log('Columns, ', self._columns);

      // create data
      for (i=0; i<dataArray.length; i++){
        var obj = dataArray[ i];
        var values = [];
        for (var key in obj) {
          if (obj.hasOwnProperty(key)) {
            var val = obj[key];
            // use val
            values.push(val);
          }
        }
        result.push(values);
      }
      self._processedData = result;
      console.log(self._processedData[ 0]);

      console.log('Creating table');
      var columns = self._columns.join(' TEXT, ');
      columns += ' TEXT, "created_at" timestamp, "updated_at" timestamp';
      var sql='CREATE TABLE IF NOT EXISTS ' + self._tablename + ' (cid serial PRIMARY KEY, ' + columns + ');';
      self._connection.data.client.query(sql, function(err, result) {
        if (err) {
          return callback(err);
        }
        console.log('Create table result: ', result);

        insertData(callback);
      });

    });
  }

  /**
   *
   * Insert data
   *
   */
  function insertData(callback){
    console.log('Inserting data. ');
    var columns = self._columns;
    columns.push('"created_at"');
    columns.push('"updated_at"');
    columns = columns.join(', ');
    var now = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '');

    var valueLines = [];
    for (var i=0; i<self._processedData.length;i++){
      var valueLine = self._processedData[ i];
      valueLine.push(now);
      valueLine.push(now);
      valueLine.forEach(function(value,key){
        if(!value || value === '') {
          value = 'null';
        } else {
          value = String(value).replace(/'/g, "''");
          value = "'" + escape(String(value)) + "'";
        }
        valueLine[ key] = value;
      });
      valueLine = '(' + valueLine.join(', ') + ')';
      valueLines.push(valueLine);
    }
    var values = valueLines.join(', ');

    var sql = 'INSERT INTO ' + self._tablename + ' (' + columns + ') VALUES ' + values + ';';
    console.log('sql, ', sql);

    self._connection.data.client.query(sql, function(err){
      callback(err);
    });
  }



  ///**
  // * Create a file from the table
  // */
  //function createFile(cb){
  //
  //  // copy table to local tmp file
  //  var stream = dataClient.query(copyFrom('COPY ' + tablename + ' FROM STDIN'));
  //  var fileStream = fs.createReadStream(tablename + '.csv');
  //  fileStream.on('error', done);
  //  fileStream.pipe(stream).on('finish', done).on('error', done);
  //
  //  // add to cms (primary)
  //
  //  // send to s3
  //
  //  // clean up
  //
  //}



  /**
   *
   * Processing finished, update cms and return to main processor.
   *
   */
  function finish(callback){

    console.log('Finished');
    var now = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '');

    // update Job status
    var sql = 'UPDATE "Jobs" SET "status"=\'done\' WHERE id=' + self._job.id;
    self._connection.cms.client.query(sql, function(err){
      console.log('e',err);
      // update Job status
      sql = 'UPDATE "Primaries" SET "jobStatus"=\'done\', "syncDate"=\'' + now + '\' WHERE id=' + self._job.data.primaryId;
      self._connection.cms.client.query(sql, function(err){
        console.log('ee',err);
        self._connection.cms.done(self._connection.cms.client);
        self._connection.data.done(self._connection.data.client);
        callback();
      });
    });
  }


  /**
   *
   * General error handler
   *
   */
  function handleError(msg){

    console.log('___error___');
    console.log(err);
    // update Job status
    var sql = 'UPDATE "Jobs" SET "status"=\'error\', "error"=\''+ String(err) + '\' WHERE id=' + self._job.id;
    self._connection.cms.client.query(sql);

    // update Primary status
    var sql = 'UPDATE "Primaries" SET "jobStatus"=\'done\' WHERE id=' + self._job.data.primaryId;
    self._connection.cms.client.query(sql);


    self._connection.cms.done(self._connection.cms.client);
    self._connection.data.done(self._connection.data.client);

    callback(err);
  }

};
