'use strict';

var models = require('../models/index');


// Authorization
exports.canManage =function(req,res,next){
  next();
};


/**
 *
 * Get a list of jobs
 *
 */
exports.index = function(req, res) {
  models.Job.find().success(function(jobs){
    res.json(jobs);
  }).error(function(err){
    return handleError(res,err);
  });
};


/**
 *
 * Job stats
 *
 */
exports.stats = function(req,res){
  return res.json('stats');
};


/**
 *
 * Status of the worker
 *
 */
exports.status = function(req,res){
  return res.json('status');
};


/**
 *
 * Get 1 job
 *
 */
exports.show = function(req, res) {
  Job.findOne(req.params.id).success(function(jobs){
    res.json(jobs);
  }).error(function(err){
    return handleError(res,err);
  });
};


/**
 *
 * Create a new job
 *
 */
exports.create = function(req, res) {
  Job.save(req.body.job).success(function(result){
    res.json(result);
  }).error(function(err){
    return handleError(res,err);
  });
};


/**
 *
 * Update a job
 *
 */
exports.update = function(req, res) {
  Job.findOne(req.params.id).success(function(job){
    jop.updateAttributes(req.body.job).success(function(result){
      return res.json(result);
    }).error(function(err){
      return handleError(res,err);
    });
  }).error(function(err){
    return handleError(res,err);
  })
};


/**
 *
 * Delete a job
 *
 */
exports.destroy = function(req, res) {
  return res.json('Delete');
};


/**
 *
 * Job log
 *
 */
exports.jobLog = function(req,res){
  return res.json('joblog');
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
