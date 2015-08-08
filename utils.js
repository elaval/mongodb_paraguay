/*
* Funciones auxiliares para mantener colecciones en MongoDB
*
* Lectura de datos de fuentes externas:
* - readDataFromFile: Lee datos de archivo json
* - readDataFromSQLite: Lee datos de una tabla en SQLite
* - openMongoConnection: Abre conexión a Mongodb
*/

var readJSON = require("read-json");
var MongoClient = require('mongodb').MongoClient;
var sqlite3 = require('sqlite3').verbose();
var Q = require("q");
var tsv = require("node-tsv-json");

var utils = {}
/*
* readDataFromFile
* Read JSON data from specified file
*/
utils.readDataFromFile = function(filePath) {
  var deferred = Q.defer();

  readJSON(filePath, function(error, data){
      deferred.resolve(data)
  })

  return deferred.promise;
}

/*
* readDataFromFileTSV
* Read tab separated  data from specified file and convert to json
*/
utils.readDataFromFileTSV = function(filePath) {
  var deferred = Q.defer();

  tsv({
    input: filePath
    //array of arrays, 1st array is column names 
    ,parseRows: false
  }, function(err, result) {
    if(err) {
      deferred.reject(err);
    }else {
      deferred.resolve(result);

    }
  });

  return deferred.promise;
}
 


/*
* readDataFromSQLite
* Read JSON data from SQLite table
*/
utils.readDataFromSQLite = function(filePath, table) {
  var deferred = Q.defer();
  var db = new sqlite3.Database(filePath);

  db.all("SELECT * FROM "+table, function(err, rows) {
    if (err) {
      deferred.reject(err)
    } else {
      deferred.resolve(rows)
    }
  });

  return deferred.promise;
}

/*
* openMongoConnection
* 
*/
utils.openMongoConnection = function(url) {
  var deferred = Q.defer();

  // Use connect method to connect to the Server
  MongoClient.connect(url, function(err, db) {
    console.log("Connected correctly to server");

    if (err) {
      deferred.reject(err)
    } else {
      deferred.resolve(db)
    }
  });

  return deferred.promise;
}

module.exports = utils;

