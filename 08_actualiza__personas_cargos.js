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
var sqlitePath='/Users/elaval/Documents/TIDE Proyectos/World Bank Data-Paraguay/Visualizacion/data/db-paraguay.sqlite';
var tableName='v_cargos_con_descripcion';
var coleccion = 'personas';

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
  batch.concurrency(500);

  _.each(data, function(d) {
    batch.push(function(done){
      var newCargo = {
        "codigo_oferta_educativa": +d.OfertaCodigo,
        "codigo_ude":+d.UDECodigo,
        "codigo_planilla":d.PlanillaCodigo,
        "ano":+d.Ano,
        "codigo_turno":+d.TurnoCodigo,
        "descripcion_turno":d.DescripcionTurno,
        "semestre":d.Semestre,
        "linea":d.Linea,
        "jornada_trabajo":d.JornadaTrabajo,
        "codigo_cargo":+d.CargoCodigo,
        "descripcion_cargo":d.CargoDescripcion,
        "establecimiento":{}
      }

      newCargo.establecimiento.codigo_establecimiento=d.codigo_establecimiento;

      newCargo.establecimiento.ubicacion={
        codigo_departamento:d.codigo_departamento,
        nombre_departamento:d.nombre_departamento,
        codigo_distrito:d.codigo_distrito,
        nombre_distrito:d.nombre_distrito,
        codigo_zona:d.codigo_zona,
        nombre_zona:d.nombre_zona,
        codigo_barrio_localidad:d.codigo_barrio_localidad,
        nombre_barrio_localidad:d.nombre_barrio_localidad,
        loc:[d.longitud,d.latitud],
      }

      collection.update(
        { 
          "codigo_tipo_documento":+d.TiposDocumentosCodigo,
          "documento":d.PersonasCI
        },
        {$addToSet: 
          {
          "cargos": newCargo
          }       
        }, 
        {'upsert':true},
        function(err,result) {
            if (err) {
              done(err);
            } else {
              done(null,d.OfertaCodigo);      
            }
        }
      ); 

       
      
    });
  })

  // batch on progress
  // Se ejecuta cada vez que se llama a funcion "done" al interior de llamada batch
  batch.on('progress', function(e){
    var msg = "PROGRESS "+e.complete+" de "+e.total+" ("+e.percent+"%) - "+e.value+"\r";
    process.stdout.write(msg);
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

Q.all([
  utils.openMongoConnection(mongoUrl),
  utils.readDataFromSQLite(sqlitePath, tableName)
])
.spread(function(db, data) {
  myMongoDb = db;
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




