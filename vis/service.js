'use strict';
/* jshint undef: true, unused: true */
/* global angular */

/**
 * @ngdoc service
 * @name simceApp.MyDataService
 * @requires $q
 * @requires d3
 * @requires _
 * @requires $http
 *
 * @description
 * Demo
 *
 */
angular.module('tideApp')
.service('DataService',[ '$rootScope','$q', 'd3', '_', '$http',function( $rootScope, $q, d3,_, $http) {
  var myself = this;
  var dataUrl = "./data/sample_data.txt";

  var data;

  this.getData = function() {
    // deferred - use of promises to deal with async results
    var deferred = $q.defer();

    if (data) {
      deferred.resolve(data)
    } else {
      d3.tsv(dataUrl, function(err,_data) {
        if (err) {
          deferred.reject(err)
        } else {
          data = _data;
          deferred.resolve(data)
        }
      })      
    }

    
 
    return deferred.promise;
  }


  this.filter = function(filterOptions) {
    // deferred - use of promises to deal with async results
    var deferred = $q.defer();

    myself.getData()
    .then(function(data) {
      // Filter data according to filter options (Ex {'Series Code':'SE.PRM.ENRR.FE'})
      var filteredData = _.filter(data, function(d) {
        var satisfyFilter = true;

        _.each(filterOptions, function(value, key) {
          satisfyFilter = satisfyFilter && (d[key]==value);
        })

        return satisfyFilter;
      })
      deferred.resolve(filteredData)
    })
 
    return deferred.promise;
  }

  // Gets posible values for 'Series Code' attribute
  this.categories = function() {
    // deferred - use of promises to deal with async results
    var deferred = $q.defer();

    myself.getData()
    .then(function(data) {
      // Filter data according to filter options (Ex {'Series Code':'SE.PRM.ENRR.FE'})
      var groups = _.groupBy(data, function(d) {
        return d['Series Code'];
      })

      deferred.resolve(_.keys(groups));
    })
    .catch(function(err) {
      deferred.reject(err)
    })
 
    return deferred.promise;
  }


}])




