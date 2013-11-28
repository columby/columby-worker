'use strict';

/* Services */


// Demonstrate how to register services
// In this case it is a simple value service.
angular.module('columbyWorker.services', []).
  value('version', '0.1')

  /*
  * Angularjs service for interacting with Drupal's requests view resource
  */
  .factory('Status', ['$http', function($http) {
    
    return {
      startSupervisor: function(){
        var promise = $http({
          method: 'GET',
          url: './api/api.php',
          params: {
            command: 'start-supervisor'
          }
        }).then(function(response){
          return response.data;
        }); 
        return promise; 
      },
      stopSupervisor: function(){
        var promise = $http({
          method: 'GET',
          url: './api/api.php',
          params: {
            command: 'stop-supervisor'
          }
        }).then(function(response){
          return response.data;
        }); 
        return promise;
      },
      
      check: function($offset){
        var promise = $http({
          method: 'GET',
          url: './api/api.php',
          params: {
            offset: $offset
          }
        }).then(function(response){
          return response.data;
        });
        return promise;
      }
    };
  }])

  /*
  * Angularjs service for interacting with Drupal's user resource
  */
  .factory('userService',['$rootScope','$http', '$q', '$log', 'appConfig', function($rootScope, $http, $q, $log, appConfig) {
    
    // get csrf token
    // connect
    // session
    var token = '';
    var user = {};
    var session_id = null;

    return {

      isAuthenticated: function() {
        if(user.uid > 0){
          return true;
        } else {
          return false;
        }
      },

      // on each new page load, the token is requested. 
      requestToken: function() {
        // $http returns a promise, which has a then function, which also returns a promise
        return $http({
          method: 'POST',
          url: appConfig.columbyApiUrl + 'user/token.json',
          data: {},
          headers: {'Content-Type': 'application/x-www-form-urlencoded'}
        }).then(function(result){
          console.log(result.data);
          token = result.data.token;
          //$http.defaults.headers.common['X-CSRF-Token'] = token;
          return result.data;
        });
      },

      connect:function(){
        //console.log($http.defaults.headers.common['X-CSRF-Token']);
        return $http({
          method: 'POST',
          url: appConfig.columbyApiUrl + 'system/connect.json',
          data: {},
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
            //'withCredentials':'true',
            //'X-CSRF-Token': $http.defaults.headers.common['X-CSRF-Token']
          }
        }).then(function(response){
          console.log(response);
          user = response.data;
          //console.log(user);
          if (user.user.uid === 0) {
            user.isAuthenticated = false;
          } else {
            user.isAuthenticated = true;
          }
          return user;
        });
      },

      login: function(u) {
        var xsrf = $.param(u);
        var promise = $http({
          method: 'POST',
          url: appConfig.columbyApiUrl + 'user/login',
          data: xsrf,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
          }
        }).then(function (response) {
          if (response.status === 200) {
            $rootScope.user = response.data;
            $rootScope.user.isAuthenticated = true;
          };
          return response;
        }, function(data){
          return data;
        });
        return promise;
      },
      
      logout: function() {
        
        // To be sure get the csrf-token first
        $http({
          method: 'POST',
          url: appConfig.columbyApiUrl + 'user/token',
          data: {}
        })
        .success(function(data) {

          // save the token
          token = data.token;
          $http.defaults.headers.common['X-CSRF-Token'] = token;

          // Logout the current user
          var promise = $http({
            method: 'POST',
            url: appConfig.apiBaseUrl + 'user/logout',
            data: {},
          })
          .then(function(response){
            if (response.status === 200) {
              
              $rootScope.user = {
                user: {
                  uid: '0',
                  roles:  { 1: 'anonymous user' }  
                },
                isAuthenticated: 'false'
              };
              alertService.add('success', 'Je bent nu uitgelogd.');
            }
            return response;
          });
          return promise;
        });
      },

      setHttpProviderCommonHeaderToken: function(token){
        $http.defaults.headers.common['X-AUTH-TOKEN'] = token;
      }
    };
  }])
;
