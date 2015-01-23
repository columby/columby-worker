'use strict';

var fs = require('fs'),
  readline = require('readline'),
  stream = require('stream'),
  request = require('request'),
  pg = require('pg'),
  escape = require('pg-escape'),
  config = require('../../config/settings'),
  Baby = require('babyparse');



module.exports = function(job,data,done) {

  /* ----- Variables ----- */
  var dataConn = config.db.geo.uri;
  var cmsConn = config.db.cms.uri;
  var tablename = 'primary_' + data.primary.id;
  var remoteFile;
  var localFile;
  var error = false;

  // csv processing variables
  var rl;                       // Readline interface
  var lineCounter     = 0;      // Counter for the number of processed lines
  var parsingFinished = false;
  var columns         = [];     //
  var batch           = [];
  var batchInProgress = false;
  var batchSize       = 50;
  var batchedPaused   = false;
  var tableCreated    = false;


  // Check for valid primary id
  if (!data.primary && !data.primary.id){
    return handleError('No primary ID!');
  }


  /* ----- FUNCTIONS ----- */
  // Initiate
    // Download the file from S3
    // Delete table if exists
  function initiate(cb){

    // get file details from cms
    pg.connect(cmsConn, function(err,client,cdone){
      if (err){ return handleError(err); }
      // primary join distribution.downloadUrl
      var sql = 'SELECT "Files"."url" FROM "Distributions" LEFT JOIN "Files" ON "Distributions"."file_id"="Files"."id" WHERE "Distributions"."id"=' + data.primary.distribution_id + ';';
      job.log('sql: '+ sql);
      client.query(sql, function(err,result){
        cdone();
        if (err){
          return handleError(err);
        } else if(result.rowCount!==1){
          return handleError('No file defined. ');
        }

        remoteFile = result.rows[0].url;
        console.log('Remote file url: ', remoteFile);

        // Validate if csv


        pg.connect(dataConn, function (err, client, cdone) {
          if (err){ return cb(err); }
          client.query('DROP TABLE IF EXISTS ' + tablename + ';',function(err) {
            cdone();
            if (err) {
              handleError(err);
            }
            job.log('Existing table dropped.');

            // Return to main function;
            cb();
          });
        });
      })
    });
  }


  /**
   *
   * Process
   * - Create tables
   * - Upload data
   *
   */
  function process(cb){
    console.log('Initiating processing. ');

    // save remote file to local disk
    localFile = config.root + '/server/tmp/' + data.primary.id;
    var ws = fs.createWriteStream(localFile);

    // request the file from a remote server
    var rem = request(remoteFile);
    rem
      .on('data', function (chunk) {
        ws.write(chunk);
      })
      .on('finish', function (err) {
        return handleError(err);
      })
      .on('end', function () {
        var instream = fs.createReadStream(localFile);
        var outstream = new stream;
        outstream.readable = true;
        rl = readline.createInterface({
          input: instream,
          output: outstream
        });

        rl
          .on('line', function (line) {
            if (!error) {
              // Parse the line
              lineCounter++;
              var parsedLine = Baby.parse(line);
              if (parsedLine.errors.length > 0) {
                //console.log(parsedLine);
                job.log('There was an error processing.', parsedLine.errors);
                return handleError(parsedLine.errors);
              } else if (lineCounter === 1) {
                console.log('Parsing first line');
                columns = parsedLine.data[0];
                createTable();
              } else {
                var l = parsedLine.data[ 0];
                if (l.length !== columns.length){
                  console.log('Field count ' + l.length + ' does not match column count ' + columns.length + '.');
                  return handleError('Field count ' + l.length + ' does not match column count ' + columns.length + '.');
                } else {
                  // add the line to the batch
                  // batch will start after creating the table;
                  batch.push(parsedLine.data[0]);
                }
              }
            }
          })
          .on('close', function () {
            console.log('Closing readline');
            // send final lines
            parsingFinished = true;
            processBatch();
          });
      });
  }


  // Complete
  // Delete local file
  // Create download file
  function finish(cb){
    console.log('Here is the finish!');
  }


  /**
   *
   * General error handler
   *
   */
  function handleError(errorMsg){
    error=true;

    // Todo: Close stream if exist
    rl.close();

    job.log('--- ERROR ---');
    job.log(String(errorMsg));
    console.log('--- ERROR ---');
    console.log(errorMsg[0].message + ' at row ' + errorMsg[0].row);
    // Todo: send error message to cms
    pg.connect(cmsConn, function(err,client,cdone){
      var sql = 'UPDATE "Primaries" SET "jobStatus"=\'error\',"statusMsg"=\'' + errorMsg[0].message + ' at row ' + errorMsg[0].row + '\' WHERE id=' + data.primary.id + ';';
      console.log('sql', sql);
      client.query(sql, function(err,res){
        console.log(err);
        cdone();
        console.log('CMS updated.');
      });
    });

    done(errorMsg);
  }


  /**
   *
   * Main
   *
   */
  function init(){
    initiate(function(err) {
      if(err){ return handleError(err);}
      process(function(err) {
        if(err){ return handleError(err);}
        finish(function(err) {
          if(err){ return handleError(err);}
          done();
        });
      });
    });
  }


  /**
   *
   * Initiate
   *
   */
  init();


  /**
   *
   * Validate and save columns to a new database table.
   * @param columns
   *
   */
  function createTable(){
    if (!columns){
      return handleError('There was an error parsing the columns. ');
    }
    var sc = sanitizeColumnNames();
    // assume all text column types for now.
    sc = sc.join(' text, ') + ' text';

    var sql = 'CREATE TABLE IF NOT EXISTS ' + tablename + ' (cid serial PRIMARY KEY, ' + sc + ', "created_at" timestamp, "updated_at" timestamp);';
    console.log('Creating table: ', sql);

    pg.connect(dataConn, function(err, client, cdone){
      client.query(sql, function(err){
        cdone();
        if (err) {
          return handleError(err);
        }
        job.log('Table created. ');
        console.log('Table created.');
        tableCreated=true;
        // Start the processing
        processBatch();
      });
    });
  }


  /**
   *
   * Save a batch of rows to the database.
   *
   */
  function processBatch() {

    if ( (error !== true) && (tableCreated === true) && (batchInProgress===false) ){
      console.log('Processing batch, ' + batch.length + ' in queue.');
      batchInProgress = true;

      while (batch.length>0){
        var workBatch = batch.splice(0,batchSize);
        console.log('Workbatch length: ' + workBatch.length);
        console.log('Remaining batch length: ' + batch.length);
        sendRows(workBatch);
      }

      batchInProgress=false;
      console.log('Done batching. ');

      if (parsingFinished===true){
        console.log('Parsing also finished. Going to the finisher. ');
        finish();
      }
    }
  }


  // create statement;
  // insert in db;
  function sendRows(rows){
    pg.connect(dataConn,function(err,client,cdone){
      if (err) { return handleError(err); }

      var buildStatement = function(rows) {
        var params = [];
        var chunks = [];
        for(var i = 0; i < rows.length; i++) {
          var row = rows[ i];
          var valuesClause = [];
          for (var k=0;k<columns.length;k++) {
            params.push(row[ k]);
            valuesClause.push('$' + params.length);
          }
          chunks.push('(' + valuesClause.join(', ') + ')')
        }
        return {
          text: 'INSERT INTO ' + tablename + ' (' + columns.join(',') + ') VALUES ' + chunks.join(', '),
          values: params
        }
      };

      console.log('Inserting ' + rows.length + ' rows.');

      // Execute the query
      client.query(buildStatement(rows), function(err){
        cdone();
        if (err){ return handleError(err); }
        console.log('Workbarch inserted. ' + batch.count + ' items in queue. ');
      });
    });
  }



  function sanitizeColumnNames(){
    var fields = [];
    columns.forEach(function(field){
      field = '"' + field.replace(' ', '_').replace('.', '_').toLowerCase() + '"';
      fields.push(field);
    });
    columns = fields;

    return columns;
  }

};
