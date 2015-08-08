/*
* carga_ListaEstablecimientos
* 
* Lee datos de estableciemientos en documento json establecimientos_lista.json
* y crea (actualiza) coleccion "estabecimientos".
*
* Genera subdocuemntos con ubicacion y zona
*/
var _ = require('underscore');
var Q = require("q");
var Batch = require('batch');
var utils = require("./utils");

var mongoUrl='mongodb://localhost:27017/paraguay';
//var sqlitePath='/Users/elaval/Documents/TIDE Proyectos/World Bank Data-Paraguay/Visualizacion/data/db-paraguay.sqlite';
//var tableName='dbo_OfertasEducativas';
var datafile = '../datasource/requerimientos_aulas_lista.json';
var coleccion = 'personas';


var marcaFormacionPostgrado = function(collection) {
  var deferred = Q.defer();

  var docentes = collection.update(
    // Marcar docentes que tengan formación de posgrado (24 o 25)
    {"formacion.codigo_formacion":{$in:[24,25]}},
    {$set: {
      "nivel_de_formacion.postgrado":true
    }},
    {multi:true},
    function(err,res) {
      deferred.resolve(true)
    }

  )

  return deferred.promise;
}

var marcaFormacionDocente = function(collection) {
  var deferred = Q.defer();

  var docentes = collection.update(
    // Marcar docentes que tengan formación docente
    {"formacion.codigo_formacion":{$in:[7,8,11,12,13,14,15,16]}},
    {$set: {
      "nivel_de_formacion.docente":true
    }},
    {multi:true},
    function(err,res) {
      deferred.resolve(true)
    }

  )

  return deferred.promise;
}

var marcaFormacionDocenteMedia = function(collection) {
  var deferred = Q.defer();

  var docentes = collection.update(
    // Marcar docentes que tengan formación docente
    {"formacion.codigo_formacion":{$in:[8,12,16]}},
    {$set: {
      "nivel_de_formacion.docente_media":true
    }},
    {multi:true},
    function(err,res) {
      deferred.resolve(true)
    }

  )

  return deferred.promise;
}

var marcaFormacionUniversitaria = function(collection) {
  var deferred = Q.defer();

  var docentes = collection.update(
    // Marcar docentes que tengan formación de posgrado (24 o 25)
    {"formacion.codigo_formacion":{$in:[19,24,25]}},
    {$set: {
      "nivel_de_formacion.universitaria":true
    }},
    {multi:true},
    function(err,res) {
      deferred.resolve(true)
    }

  )

  return deferred.promise;
}

var marcaFormacionCompatibleEducacionMedia = function(collection) {
  var deferred = Q.defer();

  var docentes = collection.update(
    // Marcar docentes que tengan formación de posgrado (24 o 25)
    {$or:[
      // Compatible si cumple alguna de las siguientes condiciones
      // Univeristaria y educacion
      {$and:[{ "formacion.codigo_formacion":{$in:[7,9,10,11,13,14,15]} }, {"formacion.codigo_formacion":{$in:[19,24,25]}}]},
      
      // Educacion media
      {"formacion.codigo_formacion":{$in:[8,12,16]}},


      //Universitaria y habilitacion pedagogica
      {$and:[{ "formacion.habilitacion_pedagogica":true }, {"formacion.codigo_formacion":{$in:[19,24,25]}}]},

    ]},
    {$set: {
      "nivel_de_formacion.compatible_educacion_media":true
    }},
    {multi:true},
    function(err,res) {
      deferred.resolve(true)
    }

  )

  return deferred.promise;
}


/*
var basicaIncompleta = f04 > 0 ? 1 : 0;
var basica = (f05+f06+f07+f08+f09+f10+f11+f12+f13+f14+f15+f16+f17+f18+f19+f24+f25) > 0 ? 1 : 0;
var media = (f06+f07+f08+f11+f12+f13+f14+f15+f16+f17+f18+f19+f24+f25) > 0 ? 1 : 0;
var docente = (f07+f08+f11+f12+f13+f14+f15+f16) > 0 ? 1 : 0;
var especializacionDocente = docente * f20 > 0 ? 1 : 0;
var tecnico = f18 > 0 ? 1 : 0;
var especializacionTecnica = tecnico * f21 > 0 ? 1 : 0;
var militarPolicial = f17 > 0 ? 1 : 0;
var especializacionMilitarPolicial = militarPolicial * f22 > 0 ? 1 : 0;
var universitario = (f19 + f24 + f25) > 0 ? 1 : 0
var especializacionUniversitaria = universitario * f23 > 0 ? 1 : 0;
var magister = f24 > 0 ? 1 : 0;
var doctorado = f25 > 0 ? 1 : 0;
var noEspecificado = (f04+f05+f06+f07+f08+f09+f10+f11+f12+f13+f14+f15+f16+f17+f18+f19+f24+f25) == 0 ? 1 : 0;
var compatibleEdMedia = f08+f12+f16+universitario*(f07+f09+f10+f11+f13+f14+f15)+universitario*PersonasHabilitacionPedagogica > 0 ? 1 : 0;
*/


var myMongoDb;

utils.openMongoConnection(mongoUrl)
.then(function(db) {
  myMongoDb = db;
  var collection = db.collection(coleccion);  
  return Q.all([
    marcaFormacionPostgrado(collection),
    marcaFormacionDocente(collection),
    marcaFormacionUniversitaria(collection),
    marcaFormacionCompatibleEducacionMedia(collection),
    marcaFormacionDocenteMedia(collection)
    ])
})
.then(function(result) {
  console.log("END");
})
.catch(function(err) {
  console.log("ERROR",err)
})
.fin(function() {
  myMongoDb.close();
})



