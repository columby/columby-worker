'use strict';

/* Controllers */

angular.module('columbyWorker.controllers', [])
  
  .controller('HomeCtrl', ['$scope', 'Status', function ($scope, Status) {
    $scope.message = '';
    $scope.offset='0'; 
    $scope.newMessage = '';
    
    var timer = setInterval(function(){
      $scope.timer = true;
      $scope.message += $scope.newMessage;
      $scope.newMessage = '';

      Status.check($scope.offset).then(function(d){
        if ($scope.timer === true) {
          $scope.timer = false;
        } else {
          $scope.timer = true;
        }

        if ($scope.message == '') {
          // trim to last 100 char for first call
          $scope.newMessage = '... [trimmed] ...<br/>' + d.message.substr((d.message.length-300),300);
        } else {
          $scope.newMessage = d.message;
        }
        //store location in file
        $scope.offset = d.offset;
      });
    }, 2000);

    $scope.startSupervisor = function(){
      console.log('start');
      Status.startSupervisor().then(function(d){
        console.log(d);
      });
    };

    $scope.stopSupervisor = function(){
      console.log('stop');
      Status.stopSupervisor().then(function(d){
        console.log(d);
      });
    };
    
    $scope.logout = function(){
      userService.logout().then(function(d){
        console.log(d);
      })
    }
  }])

  /**
   * Login controller
   **/
  .controller('LoginCtrl', ['$rootScope', '$scope', '$location', 'userService', function ($rootScope, $scope, $location, userService) {
    
    // login button click
    $scope.login = function(){
      // turn off error message
      $scope.error = false;
      // login with user service
      userService.login($scope.credentials).then(function(d) {
        console.log(d);

        // check response status
        switch(d.status) {
          // Error logging in
          case 401:
            //alertService.add('danger', 'Ongeldige gebruikersnaam of wachtwoord. ');
            $scope.credentials.password = '';
          break;
          // login success
          case 200:
            $rootScope.user = d.data;
            
            //alertService.add("success", "Je bent nu ingelogd.");
            
            // Go to homepage
            $location.path('/');
          break;
        }
      });
    },

    $scope.logout = function(){
      userService.logout().then(function(d){
        console.log(d);
      })
    }

  }])


;