'use strict';

/**
 * @ngdoc function
 * @name columbyworkerApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the columbyworkerApp
 */
angular.module('columbyworkerApp')

  .controller('MainController', function ($scope, WorkerService) {

    WorkerService.stats().then(function(result){
      $scope.stats = result;
      console.log($scope.stats);
    });

    WorkerService.jobs().then(function(jobs){
      $scope.jobs = jobs;
    });

  });
