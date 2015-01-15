'use strict';

var jwt = require('jwt-simple'),
    moment = require('moment'),
    basicAuth = require('basic-auth');
var config = require('../../config/settings');


exports.basic = function(req,res,next) {
  var user = basicAuth(req);

  function unauthorized(res) {
    res.set('WWW-Authenticate', 'Basic realm=Authorization Required');
    return res.sendStatus(401);
  }

  if (!user || !user.name || !user.pass) {
    return unauthorized(res);
  }

  if (user.name === config.auth.user && user.pass === config.auth.pass) {
    return next();
  } else {
    return unauthorized(res);
  }
};

exports.validateToken = function(req,res,next){
  if (req.headers.authorization){

    var token = req.headers.authorization.split(' ')[1];

    var payload;

    try {
      payload = jwt.decode(token, config.jwt.secret);
    }
    catch(err){
      return res.json({status:'error', message:'No valid token found.'});
    }

    if (payload){
      next();
    }

  } else {
    return res.json({status:'error', message:'No token found.'});
  }
};
