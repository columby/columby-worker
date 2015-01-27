'use strict';

var models = require('../models/index');

// Worker is busy
var processing = false;



/** -------- AUTHORIZATION ---------------------------- **/
exports.canManage =function(req,res,next){

  // get user
  if (!req.jwt || !req.jwt.sub){
    return res.status(400).json({status:'Unauthorized.'});
  }
  models.User.findOne(req.jwt.sub).then(function(user){
    // check permission
    if (user.roles.indexOf('admin')!==-1){
      next();
    } else {
      return res.status(400).json({status:'Unauthorized. Not admin'});
    }
  }).catch(function(err){
    return res.status(400).json({status:'error',msg:err});
  });
};




function finish(job){

}



/** -------- HELPER FUNCTIONS -------------------------- **/
function validType(type){
  var types=['csv','arcgis','fortes'];
  return (types.indexOf(type) !== -1);
}

function validStatus(status){
  var statuslist = ['active','processing','error','done'];
  return (statuslist.indexOf(status) !== -1);
}



/** -------- API FUNTIONS ---------------------------- **/

/**
 * API home
 */
exports.home = function(req,res){
  return res.json({
    api: 'Columby worker',
    version: '1',
    endpoints: {
      api: {
        'status [GET]': 'worker status',
        'stats [GET]': 'worker stats',
        'job [GET]': 'Get job listing',
        'job [POST]': 'Create a new job.',
        'job/:id [PUT]': 'Update a job.',
        'job/:id [DELETE]': 'Delete a job.',
        'job/:id/log [GET]': 'Get a job log.'
      }
    }
  });
};


/**
 *
 * Job stats
 *
 */
exports.stats = function(req,res){
  return res.json({
    description: 'Stats of past 30 days',
    stats: {
      total: {
        jobs: 0,
        error: 0,
        done: 0
      },
      current: {
        Active: 0,
        processing: 0
      }
    }
  });
};


/**
 *
 * Status of the worker
 *
 */
exports.status = function(req,res){
  return res.json({
    status: 'ok',
    inProgress: processing,
    queueSize: 0
  });
};


/**
 *
 * Get a list of jobs
 *
 */
exports.index = function(req, res) {
  var limit = req.query.limit || 50;
  if (limit>100){ limit=50; }
  var offset = req.query.offset || 0;
  var filter = {};
  if (req.query.status){
    filter.status = req.query.status;
  }
  if (req.query.datasetId){
    filter.dataset_id = req.query.datasetId;
  }
  if (req.query.type){
    filter.type = req.query.type;
  }

  models.Job.findAll({
    where: filter,
    order: 'id DESC',
    offset: offset,
    limit: limit
  }).then(function(jobs){
    res.json(jobs);
  }).catch(function(err){
    return handleError(res,err);
  });
};


/**
 *
 * Get 1 job
 *
 */
exports.show = function(req, res) {
  models.Job.findOne(req.params.id).then(function(job){
    res.json({job: job});
  }).catch(function(err){
    return handleError(res,err);
  });
};


/**
 *
 * Create a new job
 *
 */
exports.create = function(req, res) {

  if  (!req.body.jobType || !req.body.datasetId) {
    return handleError(res,'Not a valid job object found. ');
  }

  if (!validType(req.body.jobType)){
    return handleError(res, 'Not a valid job type');
  }

  // Get job parameters
  var job = {
    type: req.body.jobType,
    dataset_id: req.body.datasetId
  };

  models.Job.create(job).then(function(result){
    res.json(result);
  }).catch(function(err){
    return handleError(res,err);
  });
};


/**
 *
 * Update a job
 *
 */
exports.update = function(req, res) {
  // check for valid status
  if (req.body.status && !validStatus(req.body.status)) {
    return handleError(res,'Not a valid status. ');
  }

  Job.findOne(req.params.id).then(function(job){
    job.updateAttributes(req.body).then(function(result){
      return res.json(result);
    }).catch(function(err){
      return handleError(res,err);
    });
  }).catch(function(err){
    return handleError(res,err);
  })
};


/**
 *
 * Delete a job
 *
 */
exports.destroy = function(req, res) {
  models.Job.findOne(req.params.id).then(function(job){
    job.destroy().then(function(destroyed){
      return res.json(destroyed);
    }).catch(function(err){
      handleError(res,err);
    });
  }).catch(function(err){
    handleError(res,err);
  });
};


/**
 *
 * Job log
 *
 */
exports.jobLog = function(req,res){
  models.Job.findOne(req.params.id).then(function(job){
    return res.json({
      jobId: job.id,
      status: job.status,
      log:job.log
    });
  }).catch(function(err){
    handleError(res,err);
  });
};


/**
 *
 * General error handler
 *
 */
function handleError(res, err) {
  console.log('Error: ', err);
  return res.send(500, err);
}
