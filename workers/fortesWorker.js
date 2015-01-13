'use strict';

var request = require('request'),
  pg = require('pg'),
  escape = require('pg-escape'),
  settings = require('../config/settings');


module.exports = function(job,data,done) {

  var cmsClient = new pg.Client(settings.db.cms.uri);
  var dataClient = new pg.Client(settings.db.geo.uri);

  var tablename = 'primary_' + data.primary.id;
  var processedColumns
  var processedData;

  // Connect with database
  function connect(cb){
    cmsClient.connect(function(err){
      if(err){
        cb(null,err);
      } else {
        dataClient.connect(function(err) {
          if (err) {
            cb(null,err);
          } else {
            cb(true);
          }
        })
      }
    });
  }


  function processData(data,cb){
    // transform into array
    var dataArray = [];
    for( var i in data ) {
      if (data.hasOwnProperty(i)){
        dataArray.push(data[i]);
      }
    }

    var result = [];

    // create columns
    processedColumns = Object.keys(dataArray[ 0]);;

    // create data
    for (var i=0;i<dataArray.length; i++){
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
    processedData = result;
    cb(true);
  }

  // Fetch data
  function fetchData(cb){

    var options={
      url: settings.fortes.host+'/opendata',
      auth: {
        user: settings.fortes.username,
        pass: settings.fortes.password
      }
    };

    request.get(settings.fortes.url, {
      'auth': {
        'user': settings.fortes.username,
        'pass': settings.fortes.password,
        'sendImmediately': false
      }
    },function(error, response, body){
      var data = JSON.parse(body);
      processData(data, function(res,err){
        cb(true);
      });
    });
  }

  // Delete columby data-table
  function deleteTable(cb){
    dataClient.query('DROP TABLE IF EXISTS ' + tablename + ';',function(err, result) {
      if(err) {
        cb(null,'error create table if not exist.');
      } else {
        cb(true);
      }
    });
  }

  // Create new table
  function createTable(cb){
    console.log('Creating table');
    var columns = processedColumns;
    columns = columns.join(' TEXT, ');
    columns += ' TEXT, "createdAt" timestamp, "updatedAt" timestamp';
    dataClient.query('CREATE TABLE IF NOT EXISTS ' + tablename + ' (cid serial PRIMARY KEY, ' + columns + ');',function(err, result) {
      if (err) {
        cb(null, err);
      } else {
        cb(true);
      }
    });
  }

  // Insert data
  function insertData(cb){
    var columns = processedColumns;
    columns.push('"createdAt"');
    columns.push('"updatedAt"');
    columns = columns.join(', ');
    var now = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '');

    var valueLines = [];
    for (var i=0;i<processedData.length;i++){
      var valueLine = processedData[ i];
      valueLine.push(now);
      valueLine.push(now);
      valueLine.forEach(function(value,key){
        if(!value || value === "") {
          value = "null";
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

    var sql = 'INSERT INTO ' + tablename + ' (' + columns + ') VALUES ' + values + ';';

    dataClient.query(sql, function(err,res){
      if (err) {
        cb(null,err);
      } else {
        cb(true);
      }
    });
  }

  // finish
  function finish(cb){
    console.log('All is done, closing connection. ');
    cmsClient.end();
    dataClient.end();

    cb(true);
  }

  /**
   *
   * General error handler
   *
   * @param err
   * @param done
   */
  function handleError(err, done){
    console.log('Handle error.');
    console.log(err);
    cmsClient.end();
    dataClient.end();
  }


  /**
   *
   * Init
   *
   */
  console.log('Starging fortes worker.');
  connect(function(res,err){
    if (err) return handleError(err, done);

    fetchData(function(res,err) {
      if (err) return handleError(err);

      deleteTable(function(res,err) {
        if (err) return handleError(err);

        createTable(function(res,err) {
          if (err) return handleError(err);

          insertData(function(res,err) {
            if (err) return handleError(err);

            finish(function(res,err) {
              if (err) return handleError(err);
              console.log('Done!');
              done();
            })
          })
        })
      })
    })
  });
};
