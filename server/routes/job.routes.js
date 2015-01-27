'use strict';

var express = require('express'),
    jobCtrl = require('./../controllers/job.controller.js'),
    auth    = require('./../controllers/auth.controller.js'),
    router  = express.Router();


module.exports = function(app) {

  // get job listing
  router.get('/'       , auth.checkJWT, jobCtrl.home);
  router.get('/status' , auth.checkJWT, jobCtrl.status);
  router.get('/stats'  , auth.checkJWT, jobCtrl.stats);

  // get specific job
  router.get('/job'        , auth.checkJWT, jobCtrl.index);
  router.get('/job/:id'    , auth.checkJWT, jobCtrl.show);
  router.get('/job/:id/log', auth.checkJWT, jobCtrl.jobLog);

  router.post('/job'       , auth.checkJWT, jobCtrl.canManage, jobCtrl.create);

  router.put('/job/:id'    , auth.checkJWT, jobCtrl.canManage, jobCtrl.update);

  router.delete('/job/:id' , auth.checkJWT, jobCtrl.canManage, jobCtrl.destroy);


  app.use('/api', router);

};
