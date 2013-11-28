'use strict';


// Declare app level module which depends on filters, and services
angular.module('columbyWorker', [
  'ngRoute',
  'ngSanitize',
  'columbyWorker.filters',
  'columbyWorker.services',
  'columbyWorker.directives',
  'columbyWorker.controllers'
]).

config(['$routeProvider', function($routeProvider) {
  $routeProvider.when('/', {templateUrl: 'partials/home.html', controller: 'HomeCtrl'})

  // User
  .when('/login', {templateUrl: 'partials/user/login.html', controller: 'LoginCtrl'})
  .when('/account', {templateUrl: 'partials/user/account.html', controller: 'UserAccountCtrl'})

  
  //$routeProvider.when('/view2', {templateUrl: 'partials/partial2.html', controller: 'MyCtrl2'});
  $routeProvider.otherwise({redirectTo: '/view1'});
}])

.constant('appConfig', {
  columbyApiUrl: 'http://92.63.169.12/api/v1/',
  workerApiUrl: './api/'
})

/*
.config(['$httpProvider', function($httpProvider) {
  $httpProvider.defaults.transformRequest = function(data){
    if (data === undefined) {
      return data;
    }
    return $.param(data);
  };
  
  $httpProvider.defaults.headers.post['Content-Type'] = '' + 'application/x-www-form-urlencoded; charset=UTF-8';
  
  delete $httpProvider.defaults.headers.common["X-Requested-With"]
}])
*/

/*
.config(['$sceDelegateProvider', function($sceDelegateProvider) {
  $sceDelegateProvider.resourceUrlWhitelist(['self', 'http://localhost:8888/**', 'http://92.63.169.12/**']);
}])
*/

// Connect to API at startup
.run(function($rootScope, userService) {
  
  $rootScope.$on('tokenSuccess', function(){
    userService.connect().then(function(c){
      console.log(c);
      // user object received
      $rootScope.user = c;
      $rootScope.$broadcast('userSuccess', 'User Received.');
    }, function(error){
      console.log(error);
    });
  });


  // Connect to API and check if user is logged in
  // First, request XCRF Token (needed for already logged-in users)
  userService.requestToken().then(function(t){
    if (!t.token) {
      $rootScope.$broadcast('tokenError', 'Error getting token.');
    } else {
      console.log('token received');
      $rootScope.$broadcast('tokenSuccess', 'Received token.');
    }
  });
})
;
