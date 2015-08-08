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
var tableName='v_personas';
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

      // Funcion auxiliar que agrega 0s al inicio del numero 
      // Ej 12345 -> 0012345
      function pad(num, size) {
          var s = "0000000" + num;
          return s.substr(s.length-size);
      }


      collection.update(
        { 
          "codigo_tipo_documento":+d.TiposDocumentosCodigo,
          "documento":d.PersonasCI
        },
        {$set: 
          { 
            "codigo_tipo_documento":d.TiposDocumentosCodigo,
            "documento":d.PersonasCI,
            "apellidos":d.PersonasApellidos,
            "nombres":d.PersonasNombres,
            "codigo_pais":+d.PaisCodigo,
            "descripcion_pais":d.PaisDescripcion,
            "nacionalidad":d.PaisNacionalidad,
            "numero_matricula_prof":+d.PersonaNroMatriculaProf,
            "sexo":d.PersonasSexo,
            "fecha_nacimiento":d.PersonasFechaNascimiento,
            "grado_curso":d.PersonasGradoCurso,
            "codigo_niveles_funcionario":d.NivelesFuncionarioCodigo,
            "concluyo":d.PersonasConcluyo,
            "anos_servicio_sector_publico":d.PersonasAnoServicioSecPublico,
            "anos_servicio_sector_publico_y_privado":d.PersonasAnoServSecPublicoyPriv,
            "didactica_universitaria":d.PersonasDidacticaUniversitaria,
            "habilitacion_pedagogica":d.PersonasHabilitacionPedagogica=="1 - Si",
            "es_supervisor":d.EsSupervisor=="1 - Si"
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




