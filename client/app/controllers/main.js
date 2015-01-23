'use strict';

/**
 * @ngdoc function
 * @name columbyworkerApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the columbyworkerApp
 */
angular.module('columbyworkerApp')

  .controller('MainController', function ($scope, KueService) {

    KueService.stats().then(function(result){
      $scope.stats = result;
      console.log($scope.stats);
    });

  });
