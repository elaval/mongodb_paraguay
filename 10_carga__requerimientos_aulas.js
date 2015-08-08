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
var coleccion = 'establecimientos';


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

      var newRequerimiento = { 
        periodo: d.periodo,
        numero_prioridad: +d.numero_prioridad,
        codigo_institucion: +d.codigo_institucion,
        nivel_educativo_beneficiado: d.nivel_educativo_beneficiado,
        cuenta_espacio_para_construccion: d.cuenta_espacio_para_construccion,
        tipo_requerimiento_infraestructura: d.tipo_requerimiento_infraestructura,
        cantidad_requerida: +d.cantidad_requerida,
        justificacion: d.justificacion,
        numero_beneficiados: +d.numero_beneficiados,
        requerimiento_aula_id: +d.requerimiento_aula_id,
        institucion_id: +d.institucion_id
      }


      collection.update(
        { "codigo_establecimiento" : +d.codigo_establecimiento }, 
        {$addToSet: 
          {
          "requerimientos_fonacide_aulas":newRequerimiento
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



