'use strict';
/* jshint undef: true, unused: true */
/* global angular */




/**
 * @ngdoc controller
 * @name chilecompraApp.controller:CarrerasController
 * @requires $scope
 * @requires chilecompraApp.CarrerasDataService
 *
 * @property {array} colorOptions Array with options for colorAttributes
 * @property {string} colorAttribute Selected color attribute
 * @property {array} data Array with student data for the selected career & semester
 * @property {int} n Number of students in the selected data array
 * @property {int} maxCarreras Maximum number of carreras to be displayed when filtraTopCarreras is true
 * @property {array} semestres Array with the semesters options to be chosen
 * @property {string} selectedSemestre Selected semester for data selection
 * @property {string} psuValido Flag to select only data values with a valid psu score (prom_paa>0)
 * @property {string} loading Flag to show a "loading" message when its value is true
 * @description
 *
 * Controller for Carreras explorer
 *
 */
angular.module('tideApp')
.controller('AppController', ['$scope','$http','$timeout','$interval','$modal','_','d3', 'DataService',function ($scope,$http,$timeout,$interval,$modal,_,d3, dataService) {
	var myself = this;
    this.loading = false;
    this.data = [];
    this.categories = [];

    this.years = [
    '2005 [YR2005]','2006 [YR2006]','2007 [YR2007]','2008 [YR2008]','2009 [YR2009]','2010 [YR2010]','2011 [YR2011]','2012 [YR2012]','2013 [YR2013]','2014 [YR2014]'
    ]
    this.selectedYear = _.last(this.years);

    this.changeCategorie = function(categorie) {
        dataService.filter({'Series Code':categorie})
        .then(function(data) {
            myself.data = data;
        })
        .catch(function(err) {
            console.log(err)
        })
    }

    dataService.categories()
    .then(function(categories) {
        myself.categories = categories;
    })

    dataService.filter({'Series Code':'SE.PRM.ENRR.FE'})
    .then(function(data) {
        myself.data = data;
    })
    .catch(function(err) {
        console.log(err)
    })


}]);
