'use strict';

/**
 * Dependencies
 */
var config = require('../config/config'),
  jwt    = require('jwt-simple'),
  moment = require('moment');


/**
 *
 * Check if a user's jwt token is present
 * And add the user id to req
 *
 */
exports.checkJWT = function(req,res,next){
  console.log('checking jwt');
  if (!req.user) { req.user={}; }
  // Decode the token if present
  if (req.headers.authorization){
    console.log('header', req.headers.authorization);
    var token = req.headers.authorization.split(' ')[1];
    console.log('token', token);
    var payload = jwt.decode(token, config.jwt.secret);
    console.log('payload', payload);
    // Check token expiration date
    if (payload.exp <= moment().unix()) {
      console.log('Token has expired');
    }
    // Attach user id to req
    if (!payload.sub) {
      console.log('No user found from token');
    }
    req.jwt = payload;
  }

  next();
};
