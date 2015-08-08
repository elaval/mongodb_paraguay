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
var datafile = '../datasource/MateriasTitulos.txt';
var coleccion = 'personas';

var myMongoDb;
var diccionarioMaterias;

var setDiccionarioMateriasTitulos = function(datafile) {
  var deferred = Q.defer();

  utils.readDataFromFileTSV(datafile)
  .then(function(data) {
    
    /*
    data es un arreglo del tipo
    [{ idMateria: '1590',
      materia: 'DISEÑO MECANICO',
      idTitulo: '47',
      titulo: '' },
    { idMateria: '1590',
      materia: 'DISEÑO MECANICO',
      idTitulo: '249',
      titulo: '' },
    { idMateria: '1590',
      materia: 'DISEÑO MECANICO',
      idTitulo: '338',
      titulo: '' },
      ...
    ]
    */

      var materias = _.groupBy(data, function(d) {
        return d.idMateria;
      })

      // Genera dicconario con los titulos correspondientes a cada materia
      //'idMateria':['idTitulo1','idTitulo1',... ]

      _.each(materias, function(value,key) {
        materias[key] = _.map(value, function(d) {return +d.idTitulo})
      })

      deferred.resolve(materias)
    })


  return deferred.promise;
}

var obtieneMateriasCompatiblesPorPersona = function(collection,diccionarioMaterias) {
  var deferred = Q.defer();


  // Almacena una lista de todas las materias para las que una persona tiene título compatible
  var materiasPorPersona = {};

  var promises = [];

  var total = _.keys(diccionarioMaterias).length;
  var contador = 0;

  console.log("Determinando materias compatibles por persona");
  console.log("");

  _.each(diccionarioMaterias, function(titulos,idMateria) {
    var deferred2 = Q.defer();

    //Encuentra a todos los docentes que imparten la materia y que tienen titulo compatible
    collection.find(
      {$and:[{"materias.codigo_materia":+idMateria, "titulos.codigo_titulo":{$in:titulos}}]}
    )
    .toArray(function(err,personas) {
      var progressMsg = "PROGRESS "+(++contador)+" de "+total+" ("+Math.floor(100*contador/total)+"%) - "+idMateria+"("+titulos.length+")           \r";
      process.stdout.write(progressMsg);
      
      _.each(personas, function(persona) {

        // id auxiliar (tipo+"-"+documento) usado como clave para diccionario de personas
        var idPersona = persona.codigo_tipo_documento+"-"+persona.documento

        // Si es primera materia de la persona, crear el objeto asociado
        if (!materiasPorPersona[idPersona]) {
          materiasPorPersona[idPersona] = {
            'codigo_tipo_documento':+persona.codigo_tipo_documento,
            'documento':persona.documento,
            'materias_compatibles':[]
          }
        }

        materiasPorPersona[idPersona].materias_compatibles.push(+idMateria);
      })
      deferred2.resolve(idMateria);

    })

    promises.push(deferred2.promise);
  })

  Q.all(promises)
  .then(function() {
    console.log("");
    console.log("Diccionario con materias por persona construido");
    console.log(materiasPorPersona);
    deferred.resolve(materiasPorPersona);
  })


  return deferred.promise;
}


var marcarMateriasCompatibles = function(collection,materiasPorPersona) {
  var deferred = Q.defer();

  var total = _.keys(materiasPorPersona).length;
  var contador = 0;

  var promises = []

  // Iterar por cada persona para la cual se tienen materias compatibles
  _.each(materiasPorPersona, function(d) {
    var deferred2   = Q.defer();
    var codigo_tipo_documento = d.codigo_tipo_documento;
    var documento = d.documento;
    var materias_compatibles = d.materias_compatibles;

    // Obtener documento correspondiente a la persona
    collection.findOne({'codigo_tipo_documento':+codigo_tipo_documento, 'documento':documento}, function(err, doc) {
      
      if (documento == 4662283) {
        console.log("")
        console.log("FOUND");
        console.log("")
      }

      if (err) {
        console.log(err);
        deferred2.reject(err);
      } else {

        if (!doc.materias) console.log("ERROR - NO MATERIAS", documento)
        // Iterar por materias y actualizar indicador de materia compatible con titulos
        _.each(doc.materias, function(materia) {
          if (documento == 4662283) {
            console.log("")
            console.log("Materia ",materia);
            console.log("")
          }
          if (_.contains(materias_compatibles, +materia.codigo_materia)) {
            materia.compatible_con_titulos=true;
          } else {
            materia.compatible_con_titulos=false;
          }
          
        })

          if (documento == 4662283) {
            console.log("")
            console.log("Materia ",doc.materias);
            console.log("")
          }

        collection.update(
          {'codigo_tipo_documento':+codigo_tipo_documento, 'documento':documento},
          doc,
          function(err,res) {
            var progressMsg = "PROGRESS "+(++contador)+" de "+total+" ("+Math.floor(100*contador/total)+"%) - "+documento+"("+materias_compatibles.length+")           \r";
            process.stdout.write(progressMsg);
            deferred2.resolve(doc.documento);
          }

        )
        // Guardar nueva version del docuemnto de la persona
        //collection.save(doc);


      }
  

    })

    promises.push(deferred2.promise);
  })

  Q.all(promises)
  .then(function(d) {
    deferred.resolve(d.length);
  })
   
  return deferred.promise;
}


utils.openMongoConnection(mongoUrl)
.then(function(db) {
  myMongoDb = db; 
  return setDiccionarioMateriasTitulos(datafile);
    
})
.then(function(result) {
  diccionarioMaterias = result;
  var collection = myMongoDb.collection(coleccion); 

  return obtieneMateriasCompatiblesPorPersona(collection,diccionarioMaterias);
})
.then(function(materiasPorPersona) {
  var collection = myMongoDb.collection(coleccion); 
  return marcarMateriasCompatibles(collection, materiasPorPersona)
})
.then(function(num) {
  console.log("")
  console.log("Actualizadas "+num+" personas");
  console.log("END");
})
.catch(function(err) {
  console.log("ERROR",err)
})
.fin(function() {
  myMongoDb.close();
})



