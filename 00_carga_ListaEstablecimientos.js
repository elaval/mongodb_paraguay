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
var datafile = '../datasource/establecimientos_lista.json';
var coleccion = 'establecimientos';

/*
* Convierte datos de atributo geom a par latitud,longitug
*/
var convertGeom2Loc = function(geom) {
  var latitud,
    longitud;

  if (geom) {
    var aux = geom.split("(")[1];
    var aux = aux.split(")")[0];
    var aux = aux.split(" ");
    longitud = +aux[0];
    latitud = +aux[1];
  }
  return ([longitud,latitud]);
}

var procesaColeccion = function(collection,data) {
  var deferred = Q.defer();
  var batch = new Batch;

  // Batch, permite generar una cola de procesos y
  // definir un nivel de concurrencia (para evitar saturar el proceso)
  // Se se genera una cola con funciones a ejecutar
  // batch.push(function(done) {})
  // 
  // Al terminar la ejecución de la función se debe llamar
  // a función done(err,result), para continuar con la cola de procesos
  batch.concurrency(100);

  _.each(data, function(d) {
    batch.push(function(done){

      // extrae datos de longitud y latitud de atributo geom "POINT (-56.1878813889 -24.5168777778)"
      var newGeom = convertGeom2Loc(d.geom);

      collection.update(
        { "codigo_establecimiento" : +d.codigo_establecimiento }, 
        {$set: 
          {
          "codigo_establecimiento":+d.codigo_establecimiento,
          "uri":d.uri,
          "ubicacion.codigo_departamento":+d.codigo_departamento,
          "ubicacion.nombre_departamento":d.nombre_departamento,
          "ubicacion.codigo_distrito":+d.codigo_distrito,
          "ubicacion.nombre_distrito":d.nombre_distrito,
          "ubicacion.codigo_barrio_localidad":+d.codigo_barrio_localidad,
          "ubicacion.nombre_barrio_localidad":d.nombre_barrio_localidad,
          "ubicacion.direccion":d.direccion,
          "ubicacion.coordenadas_y":+d.coordenadas_y,
          "ubicacion.coordenadas_x":+d.coordenadas_x,
          "ubicacion.latitud":d.latitud,
          "ubicacion.longitud":d.longitud,
          "ubicacion.loc":newGeom,
          "ubicacion.anho_cod_geo":+d.anho_cod_geo,
          "zona.nombre_zona":d.nombre_zona,
          "zona.codigo_zona":+d.codigo_zona
          }       
        }, 
        {'upsert':true},
        function(result) {
            //console.log("Updated the document with codigo ",d.codigo_establecimiento);
            done(null,d.codigo_establecimiento)
        });  
    });
  })

  batch.on('progress', function(e){
    var msg = "PROGRESS "+e.complete+" de "+e.total+" ("+e.percent+"%) - "+e.value+"\r";
    process.stdout.write(msg);
    //console.log("PROGRESS %s de %s (%s %) - %s",e.complete, e.total, e.percent, e.value)
  });

  batch.end(function(err, res){
    if (err) {
      console.log("ERROR",err)
    } else {
      console.log("Inserted documents into the document collection");
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
  console.log("END");
})
.catch(function(err) {
  console.log("ERROR",err)
})
.fin(function() {
  myMongoDb.close();
})



