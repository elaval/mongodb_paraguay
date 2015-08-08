/*
* actualiza_ListaInstituciones
* 
* Lee datos de instituciones en documento json directorios_instituciones_lista
* y crea (actualiza) subcoleccion (arreglo) de institucines en el docuemnto del establecimeinto respectivo de la coleccion "estabecimientos".
*
* Genera subcoleccion con datos de la institucion
*/
var _ = require('underscore');
var Q = require("q");
var Batch = require('batch');
var utils = require("./utils");

var mongoUrl='mongodb://localhost:27017/paraguay';
//var sqlitePath='/Users/elaval/Documents/TIDE Proyectos/World Bank Data-Paraguay/Visualizacion/data/db-paraguay.sqlite';
//var tableName='dbo_OfertasEducativas';
var datafile = '../datasource/directorios_instituciones_lista.json';
var coleccion = 'establecimientos';


var procesaColeccion = function(collection,data) {
  var deferred = Q.defer();
  var batch = new Batch;

  batch.concurrency(100);

  _.each(data, function(d) {
    batch.push(function(done){

    collection.update(
      { "codigo_establecimiento" : +d.codigo_establecimiento},
      {$addToSet: 
        {
        "instituciones": {
            "codigo_institucion": +d.codigo_institucion,
            "nombre_institucion": d.nombre_institucion,
            "codigo_nautilus": +d.codigo_nautilus,
            "denominacion_institucion": d.denominacion_institucion,
            "uri_institucion": d.uri_institucion
          }

        }       
      }, 
      {'upsert':true},
      function(result) {
        // Indicar fin de proceso para continuar con cola del bacth
        done(null,d.codigo_establecimiento)
      });  
    });
  })

  batch.on('progress', function(e){
    var msg = "PROGRESS "+e.complete+" de "+e.total+" ("+e.percent+"%) - "+e.value+"\r";
    process.stdout.write(msg);
  });

  batch.end(function(err, res){
    if (err) {
      console.log("ERROR",err)
    } else {
      console.log("Finalizó el proceso de la colección");
      deferred.resolve(res);
    }
  });

  return deferred.promise;
}


var myMongoDb;

utils.openMongoConnection(mongoUrl)
.then(function(db) {
  myMongoDb = db;
  return utils.readDataFromFile(datafile)
})
.then(function(data) {
  console.log("Data read", data.length)
  var collection = myMongoDb.collection(coleccion);  
  return procesaColeccion(collection,data)
})
.then(function(result) {
  console.log("FIN");
})
.catch(function(err) {
  console.log("ERROR",err)
})
.fin(function() {
  myMongoDb.close();
})
  


