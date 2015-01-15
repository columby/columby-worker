'use strict';

angular.module('columbyworkerApp')

  .service('KueService', function($http) {

    return {
      stats: function () {
        return $http({
          method: 'GET',
          url: '/stats'
        }).then(function (result) {
          console.log(result);
          return result.data;
        });
      },

      jobs: function(params){
        return $http({
          method: 'get',
          url: 'jobs',

        })
      }

    }
  });
