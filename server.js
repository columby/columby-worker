/* here we go */

var kue = require('kue'),
	request = require('request');
var arcgis = require('./workers/arcgis.js');
var jobs = kue.createQueue();



jobs.process("geoservice",function(job,done){
	geoservice(job,done)
});


function geoservice(job,done){

	var ID = job.data.ID;

	request('http://188.166.31.214:8000/api/v2/dataset/'+ID, function(error,response,body) {
        if(error) done("error getting dataset (id: "+ID+") info");
        if(!error && response.statusCode == 200){
        	var data = JSON.parse(body);
        	// start arcgis scraper
        	arcgis.go(job,data,done);
        }
    });

}

kue.app.set('title', 'Columby Worker // kue');
kue.app.listen(3000);
