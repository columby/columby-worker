'use strict';

var jwt = require('jwt-simple'),
    moment = require('moment');
var config = require('../config/settings');


exports.validateToken = function(req,res,next){

  if (req.headers.authorization){

    var token = req.headers.authorization.split(' ')[1];
    console.log(token);

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
