'use strict';
// example: http://www.denhaag.nl/ArcGIS/rest/services/Open_services/Kunstobjecten/MapServer/0

var request = require('request'),
  pg = require('pg'),
  escape = require('pg-escape'),
  config = require('../config/environment');


var ArcgisWorker = module.exports = function(){
  var self=this;

  self._job        = null; 			// job
  self._connection = null;
  self._total      = null;					// total rows external dataset
  self._version    = null;				// dataset version
  self._stats      = null;					// dataset arcgis stats
  self._batch      = null;					// all dataset ids
  self._batchSize  = 100;
  self._objectidpresent = true;
  self._tablename  = null;
  self._chunk_size = 100;		// chunk size scrape (rows per cycle)
};


ArcgisWorker.prototype.start = function(job,callback){
  var self=this;
  self._job = job;

  connect(function(err) {
    if (err) {
      handleError('There as an error connecting to the DBs.');
      //return callback(err)
    }
    // validate job data
    validateData(function(err) {
      if (err) {
        console.log('There as an error validating the data.',err);
        handleError('There as an error validating the data.');
        //return callback(err)
      }
      // data processing
      process(function (err) {
        if (err) {
          console.log('There as an error processing the data.',err);
          handleError('There as an error processing the data.');
          //return callback(err)
        }
        // finish
        finish(function (err) {
          if (err) {
            console.log('There as an error finishing.',err);
            handleError('There as an error finishing.');
            //return callback(err)
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
   *
   */
  function validateData(callback){

    if (!self._job.data.primaryId) {
      return callback('No primary ID!');
    }
    if (!self._job.data.url) {
      return callback('No access url!');
    }

    self._tablename = 'primary_' + self._job.id;

    callback();
  }


  /**
   *
   * Drop existing table and initiate
   *
   */
  function process(callback) {

    // drop existing database
    self._connection.data.client.query('DROP TABLE IF EXISTS ' + self._tablename, function (err) {
      if (err) { callback('Error create table if not exist'); }

      // Update job status
      var sql = 'UPDATE "Jobs" SET "status"=\'processing\' WHERE id=' + self._job.data.primaryId;
      self._connection.cms.client.query(sql, function (err) {
        if (err) { callback('eError updating jobstatus. ', err); }
        processData(callback);
      });
    });
  }


  function processData(callback) {
    console.log('Getting stats ...');
    console.log(self._job.data.url);
    request.get({
      url: self._job.data.url + '?f=json&pretty=true',
      json: true
    }, function (err, res, data) {
      if (err) {
        console.log(err);
        callback('Error getting stats');
      } else {
        console.log(data);
        self._stats = data;
        self._version = data.currentVersion;
        self._columns = data.fields;

        console.log('version', self._version);

        if (!self._version) {
          return callback('No current version found.');
        }

        // set batchParams based on version
        var batchParams = {
          f: 'pjson',
          where: '1=1',
          returnIdsOnly: 'false',
          text: '',
          returnGeometry: 'true',
          geometryType: 'esriGeometryEnvelope',
          spatialRel: 'esriSpatialRelIntersects',
          outFields: '*',
          outSR: '4326'
        };
        if (self.version === '10.04') {
        }
        if (self.version === '10.11') {
        }
        self._batchParams = batchParams;

        //getids
        console.log('Getting object ids ...');

        var params = {
          f: 'pjson',
          objectIds: '',
          where: '1=1',
          returnIdsOnly: 'true',
          text: '',
          returnGeometry: 'false',
          json: 'true'
        };
        request.get({
          url: self._job.data.url + '/query',
          qs: params
        }, function (err, res, data) {
          if (err) {
            console.log(err);
            callback('Error getting object ids.');
          } else {
            var data = JSON.parse(data);
            self._batch = data.objectIds;
            console.log('Received ' + data.objectIds.length + ' objectIds.');

            checkBatch(callback);
          }
        });
      }
    });
  }


  function checkBatch(callback) {
    console.log('checking batch');
    if (!self._tableCreated){
      console.log('creating table');
      // Create table
      createTable();
    } else if (self._batch.length > 0){
      console.log('Batch not empty, not processing, let\'s go!');
      processBatch(function(err){
        if (err){ callback(err);}
      });
    } else if(self._batch.length === 0){
      finish();
    }
  }

  function createTable(callback){
    var esriconvertable = {
      esriFieldTypeSmallInteger : 'TEXT',
      esriFieldTypeInteger  	  : 'TEXT',
      esriFieldTypeSingle     	: 'TEXT',
      esriFieldTypeDouble   	  : 'TEXT',
      esriFieldTypeString     	: 'TEXT',
      esriFieldTypeDate	 		    : 'TEXT',
      esriFieldTypeOID	  		  : 'TEXT',
      esriFieldTypeGeometry 	  : 'TEXT',
      esriFieldTypeBlob	 		    : 'TEXT',
      esriFieldTypeRaster   	  : 'TEXT',
      esriFieldTypeGUID	 		    : 'TEXT',
      esriFieldTypeGlobalID 	  : 'TEXT',
      esriFieldTypeXML	  		  : 'TEXT',
      Latitude	  				      : 'TEXT'
    };

    // process each field to get columns
    var columnNames = [];
    var columnTypes = [];

    self._columns.forEach(function(f) {
      // convert esri types to postgis types
      var type = esriconvertable[ f.type];
      var value = f.name;
      // create column for the type
      columnNames.push(value);
      columnTypes.push(type);
    });

    console.log(columnNames.indexOf('OBJECTID'));

    if (columnNames.indexOf('_objectid') === -1 ) {
      self._objectidpresent = false;
      columnNames.push('_objectid');
      columnTypes.push('TEXT');
    }

    columnNames = sanitizeColumnNames(columnNames);

    // Add geometry and dates
    columnNames.push('the_geom');
    columnTypes.push('geometry');
    columnNames.push('createdAt');
    columnTypes.push('timestamp');
    columnNames.push('updatedAt');
    columnTypes.push('timestamp');

    // create columns
    var columns = [];
    columnNames.forEach(function(v,k){
      columns.push( v + ' ' + columnTypes[ k]);
    });

    // create table if not exists
    var sql = 'CREATE TABLE IF NOT EXISTS ' + self._tablename + ' (cid serial PRIMARY KEY, ' + columns + ')';

    self._connection.data.client.query(sql, function(err, result) {
      if(err){
        console.log('error create table if not exist', err);
      } else {
        self._tableCreated = true;
        checkBatch();
      }
    });
  }


  function processBatch(callback) {
    console.log('processing batch ' + self._batch.length);
    if (self._batch.length>0){
      self._batchInProgress = true;
      var workBatch = self._batch.splice(0, self._batchSize);
      console.log('Workbatch length: ' + workBatch.length + ', remaining: ' + self._batch.length);
      sendRows(workBatch, function(err){
        if (err){ return callback(err);}
        processBatch();
      });
    } else {
      self._batchInProgress=false;
      checkBatch();
    }
  }

  function sendRows(rows, callback){
    console.log('sendRows');

    callback();
  }


  function finish() {
    console.log('Finished');

    // update Job status
    //var sql = 'UPDATE "Jobs" SET "status"=\'done\' WHERE id=' + self._job.id;
    //self._connection.cms.client.query(sql);

    // update Job status
    var sql = 'UPDATE "Primaries" SET "jobStatus"=\'done\' WHERE id=' + self._job.data.primaryId;
    self._connection.cms.client.query(sql);


    self._connection.cms.done(self._connection.cms.client);
    self._connection.data.done(self._connection.data.client);
    //self._rl.close();

    callback();

  }


  function sanitizeColumnNames(columns){
    var fields = [];
    columns.forEach(function(field) {
      field = '"_' + field.replace(' ', '_').replace('.', '_').toLowerCase() + '"';
      fields.push(field);
    });

    return fields;
  }


  function handleError(err){
    console.log(err);
  }

};


exports.go = function(job,data,done){

	this.end = function(message){
    job.log('Finishing job');

		if(message){
			var sql = 'UPDATE "Primaries" SET status=\'error\',"StatusMsg"=\'' + String(message).replace(/'/g, "''") + '\' WHERE id=' + self.columby_data.primary.id + ';';
			self.cmsClient.query(sql, function(err,result){
				// error query
				if(err){
					console.log(err);
				}
				self.cmsClient.end();
        self.dataClient.end();
				console.log(message);
				self.done(message);
			});
			//done(message);
		} else {
			var now = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '');
      var sql = 'UPDATE "Primaries" SET status=\'done\',"syncDate"=\'' + now + '\' WHERE id=' + self.columby_data.primary.id + ';';
			self.cmsClient.query(sql, function(err,result){
				// error query
				if(err) console.log(err);

        self.cmsClient.end();
        self.dataClient.end();
				console.log('job done!');
				self.done();
			})
		}
	};

	// running functions (in order of appearence)

	this.get_total = function(){
		var params = {"f":"pjson",
						"objectIds":"",
						"where":"1=1",
						"returnIdsOnly":"true",
						"returnCountOnly":"true",
						"text":"",
						"returnGeometry":"false"};

		request.get({url:this.url+"/query",qs:params,json:true},function(error,response,data){
			if(error) self.end("error getting total");
			else self.total = data.count;
		});
	};


  this.get_records_recursive = function(chunki){


		// GET url (for debugging)
		var str = [];
		for(var k in params){ str.push(k+"="+params[k]);};
		var get_url = this.url+"/query?"+str.join("&");
		// console.log(get_url);

		request.get({url:get_url,qs:{},json:true},function(error,response,data){
			if(error) self.end("error getting total");
			else {
				self.put_in_storage(data,ids,chunki);
			}
		});
	};

	this.put_in_storage = function(data,current_ids,chunki){
		// WRITE DATA TO DB HERE
		// where tablename = primary_ID

		if(data.features.length<1){
			self.end();
			return true;
		}

		self.columns = self.define_columns(data);



		// insert data ------------------------------------------------------------------------------------------

		// array for all value rows.
		var valueLines = [];

		for(var i=0; i<data.features.length; i++){

			var row = data.features[i];

			var values = [];
			for(var k in row.attributes) values.push(row.attributes[k]);

			// add current id if OBJECTID field is missing
			if(!self.objectidpresent) values.unshift(current_ids[i]);

			// escape string
			values.forEach(function(v,k){
				if(!v || v=="") v = "null";
				else {
					// escape ' -> ?!
					v = String(v).replace(/'/g, "''");
					v = "'"+escape(String(v))+"'";
					// v = "'"+"nothing"+"'";
				}
				values[k] = v;
			});

			// get geodata
			if(row.geometry.x && row.geometry.y){
				values.push("ST_GeomFromText('POINT("+row.geometry.x+" "+row.geometry.y+")', 4326)");
				// values.push("null");
			} else if(row.geometry.rings){
				var points = [];
				row.geometry.rings[0].forEach(function(v,k){
					points.push(v[0]+" "+v[1]);
				})
				var pointString = points.join(",");
				values.push("ST_GeomFromText('POLYGON(("+pointString+"))', 4326)");
				// values.push("null");
			} else {
				values.push("null");
			}

			// set dates
			var now = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '');
			values.push("'"+now+"'");  // createdAt
        	values.push("'"+now+"'");  // updatedAt

        	// create string from values
        	values = values.join(", ");

        	// add to valueLines

        	valueLines.push("("+values+")");
		}
		valueLines = valueLines.join(", ");

		var query = "INSERT INTO "+self.tablename+" ("+self.columnNames+") VALUES "+valueLines+";";
		self.dataClient.query(query,function(err, result) {

			if(err) {
				// console.log("+++ INSERT ERROR +++ ");
				// console.log(query.substring(0,5000));
				console.log("features length",data.features.length);
				self.end(err);
			} else {
				console.log("inserted "+current_ids.length+" rows");
				// if(chunki<self.chunks.length && chunki<self.max_test_chunks){
				self.job.data.chunk = chunki;
				if(self.job.progress) self.job.progress(chunki,self.chunks.length);
				self.get_records_recursive(chunki+1);
			}
		});
	};
};
