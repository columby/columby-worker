// example: http://www.denhaag.nl/ArcGIS/rest/services/Open_services/Kunstobjecten/MapServer/0

var request = require('request');

exports.go = function(job,data,done){

	var self = this;

	// some basic vars
	self.job = job; 			// kue job
	self.done = done; 			// kue done() function
	self.columby_data = data; 	// columby dataset metadata
	self.url; 					// arcgis dataUrl
	self.total;					// total rows external dataset
	self.version;				// dataset version
	self.stats;					// dataset arcgis stats
	self.ids;					// all dataset ids
	// self.job.data.chunk; 	// current chunk as processed
	
	// options
	self.chunk_size = 5;		// chunk size scrape (rows per cycle)
	

	// RUN:

	self.init = function(){

		// get accessUrl from primary distribution 
		for(var i = 0; i < self.columby_data.distributions.length; i++){
			if(self.columby_data.distributions[i].id==self.columby_data.primary.distribution_id){
				var url = self.columby_data.distributions[i].accessUrl;
				self.url = url;
			}
		}
		if(!url) self.done("no accessUrl found");

		// run
		self.get_stats();

		// self.get_stats();
		// self.get_ids();
		// self.get_records();

	}


	// functions (in order of appearence)

	this.get_stats = function(){
		console.log("getting stats...");

		var params = {"f":"json","pretty":"true"};
		request.get({url:this.url,qs:params,json:true},function(error,response,data){
			if(error) self.done("error getting stats");
			else {
				self.stats = data;
				self.version = data.currentVersion;
				// -->
				self.get_ids();
			}
		});
	}

	this.get_ids = function(){
		console.log("getting object ids...");
		
		var params = {"f" :"pjson",
      					"objectIds":"",
      					"where":"1=1",
      					"returnIdsOnly":"true",
      					"text":"",
      					"returnGeometry":"false"};

      	request.get({url:this.url+"/query",qs:params,json:true},function(error,response,data){
			if(error) self.done("error getting stats");
			else {
				self.ids = data.objectIds;
				console.log("split ids in chunks of "+self.chunk_size);
				self.chunks = [];
				for (var i=0; i<self.ids.length; i++) {
					var part = parseInt(i / self.chunk_size);
					if(!self.chunks[part]) self.chunks[part] = [];
				    self.chunks[part].push(self.ids[i]);
				}
				// -->
				self.get_records();
			}
		});
	}

	this.get_total = function(){
		var params = {"f":"pjson",
						"objectIds":"",
						"where":"1=1",
						"returnIdsOnly":"true",
						"returnCountOnly":"true",
						"text":"",
						"returnGeometry":"false"};

		request.get({url:this.url+"/query",qs:params,json:true},function(error,response,data){
			if(error) self.done("error getting total");
			else self.total = data.count;
		});
	}

	this.get_records = function(){
		
		console.log("get and process rows chunk by chunk...");

		// continue or start

		var start = self.job.data.chunk ? self.job.data.chunk : 0;

		var result = self.get_records_recursive(start);
	}

	this.get_records_recursive = function(i){
		console.log("get chunk #"+i+"...");
			
		var ids = self.chunks[i];
		
		if(self.version=="10.04"){
			var params = {
						"f"             : "pjson",
						"where"         : "1=1",
						"returnIdsOnly" : "false",
						"text"          : "",
						"returnGeometry": "true",
						"geometryType"  : "esriGeometryEnvelope",
						"spatialRel"    : "esriSpatialRelIntersects",
						"outFields"     : "*",
						"outSR"         : "4326", 
						"objectIds"     : ids};
		}
		
		if(self.version=="10.11"){
			var params = {
						"f"             : "pjson",
						"where"         : "1=1",
						"returnIdsOnly" : "false",
						"text"          : "",
						"returnGeometry": "true",
						"geometryType"  : "esriGeometryEnvelope",
						"spatialRel"    : "esriSpatialRelIntersects",
						"outFields"     : "*",
						"outSR"         : "4326", 
						"objectIds"     : ids}
		}
		request.get({url:this.url+"/query",qs:params,json:true},function(error,response,data){
			if(error) self.done("error getting total");
			else {
				self.temp_data = data;
				// WRITE DATA TO DB HERE
				// where tablename = primary_ID
				console.log(data.spatialReference);
				if(i<self.chunks.length && i<5){
					self.job.data.chunk = i;
					self.job.progress(i,self.chunks.length);
					self.get_records_recursive(i+1);
				}
			}
		});
	}

	this.init();

	return this;

}






// -------------------------- TEST -------------------------- //


var exampledata = { id: 158,
  shortid: 'MWLxdxJ29Pr',
  uuid: '2ae33e41-6ca1-4c7e-869b-b84fa4c178e0',
  title: 'Kunstobjecten in de openbare ruimte',
  slug: null,
  description: 'Den Haag telt circa 400 beelden in de openbare ruimte. Onder een beeld wordt hier verstaan: een driedimensionaal kunstwerk in de openbare ruimte.  Deze dataset bevat de locaties en omschrijving van beelden in de openbare ruimte in Den Haag. De dataset wordt wekelijks bijgewerkt.\n',
  private: false,
  created_at: '2014-10-24T09:22:07.000Z',
  updated_at: '2015-01-08T18:23:30.541Z',
  account_id: 311,
  headerimg_id: 1,
  distributions:
   [ { id: 178,
       shortid: 'EKrWNWRV9wl',
       title: 'Download',
       type: 'localFile',
       status: 'draft',
       valid: false,
       description: null,
       issued: null,
       modified: null,
       license: 'CC0',
       rights: null,
       accessUrl: null,
       downloadUrl: 'http://beta.columby.com/sites/default/files/datasets/2ae33e41-6ca1-4c7e-869b-b84fa4c178e0.csv',
       mediaType: null,
       format: null,
       byteSize: null,
       created_at: '2014-12-30T21:16:13.000Z',
       updated_at: '2014-12-30T21:16:13.475Z',
       account_id: null,
       dataset_id: 158,
       file_id: null },
     { id: 89,
       shortid: 'yoJg7v1zyW',
       title: 'Remote service link',
       type: 'remoteService',
       status: 'draft',
       valid: true,
       description: null,
       issued: null,
       modified: null,
       license: 'CC0',
       rights: null,
       accessUrl: 'http://www.denhaag.nl/ArcGIS/rest/services/Open_services/Kunstobjecten/MapServer/0',
       downloadUrl: null,
       mediaType: 'link',
       format: 'link',
       byteSize: null,
       created_at: '2014-12-30T21:16:12.000Z',
       updated_at: '2014-12-30T21:17:34.451Z',
       account_id: null,
       dataset_id: 158,
       file_id: null } ],
  primary:
   { id: 1,
     shortid: '14dg6e5q3K',
     status: 'draft',
     statusMsg: null,
     syncPeriod: null,
     syncDate: null,
     created_at: '2014-12-30T21:17:39.000Z',
     updated_at: '2014-12-30T21:17:39.236Z',
     dataset_id: 158,
     distribution_id: 89 },
  tags: [],
  headerImg:
   { id: 1,
     shortid: 'DMeN9xpVDl',
     type: 'image',
     filename: '527729_500330486644412_1797308598_n-26-7-1.jpeg',
     filetype: 'image/jpeg',
     title: null,
     description: null,
     url: 'https://columby-dev.s3.amazonaws.com/accounts/311/images/527729_500330486644412_1797308598_n-26-7-1.jpeg',
     status: true,
     size: 133738,
     created_at: '2014-12-30T22:20:15.000Z',
     updated_at: '2014-12-30T22:20:17.135Z',
     account_id: 311 },
  account:
   { id: 311,
     uuid: '10043b3f-8ba9-449d-b83d-f42141cd57f9',
     shortid: 'D61ANA2E7kP',
     name: 'Gemeente Den Haag',
     slug: 'gemeente-den-haag',
     description: 'De gemeente heeft Columby als distributieplatform gekozen om haar open data te publiceren. De gemeente heeft een schat aan informatie. Over alles wat zichtbaar is in de openbare ruimte, geografische data, statistieken over de bevolking en over toekomstige ontwikkelingen. Een deel hiervan publiceren wij als vrij beschikbare data. Zo stimuleren we het hergebruik van informatie, leveren we een bijdrage aan een transparante overheid en helpen we met samenwerking tussen de gemeente en haar partners. De informatie bevat geen juridisch advies en wordt alleen aangeboden voor algemene informatieve doeleinden, zonder dat voor de juistheid ervan expliciet of impliciet een garantie wordt gegeven. Als u vragen of suggesties heeft, stuur dan een mail naar opendata@denhaag.nl',
     primary: false,
     created_at: '2014-12-30T21:16:09.000Z',
     updated_at: '2014-12-30T22:28:34.032Z',
     avatar_id: 4,
     headerimg_id: 3,
     avatar:
      { id: 4,
        shortid: 'MKmdRm3Kqv',
        type: 'image',
        filename: '536979_562662583764926_784156659_n-4.jpg',
        filetype: 'image/jpeg',
        title: null,
        description: null,
        url: 'https://columby-dev.s3.amazonaws.com/accounts/311/images/536979_562662583764926_784156659_n-4.jpg',
        status: true,
        size: 29303,
        created_at: '2014-12-30T22:28:33.000Z',
        updated_at: '2014-12-30T22:28:34.016Z',
        account_id: 311 },
     headerImg:
      { id: 3,
        shortid: 'OWYE0Yx32P',
        type: 'image',
        filename: '536979_562662583764926_784156659_n-3.jpg',
        filetype: 'image/jpeg',
        title: null,
        description: null,
        url: 'https://columby-dev.s3.amazonaws.com/accounts/311/images/536979_562662583764926_784156659_n-3.jpg',
        status: true,
        size: 29303,
        created_at: '2014-12-30T22:26:11.000Z',
        updated_at: '2014-12-30T22:26:12.528Z',
        account_id: 311 } },
  references:
   [ { id: 1,
       description: 'NOS.nl - Nieuws, Sport en Evenementen op Radio, TV en Internet',
       url: 'http://nos.nl/',
       title: 'Nederlandse Omroep Stichting',
       provider_name: 'Nos',
       provider_display: 'nos.nl',
       image: 'http://nos.nl/img/social/nos.jpg?141223',
       updated_at: '2014-12-30T21:19:00.219Z',
       dataset_id: 158 } ] }

// exports.go(exampledata,function(){});


/**/