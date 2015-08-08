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

// Guarda archivo json con listado de establecimeintos como texto
var saveAsTXT = function(data) {
  var deferred = Q.defer();

  var outData = "";

  var fields = [
		"anio",
		"codigo_establecimiento",
		"codigo_departamento",
		"nombre_departamento",
		"codigo_distrito",
		"nombre_distrito",
		"codigo_zona",
		"nombre_zona",
		"codigo_barrio_localidad",
		"nombre_barrio_localidad",
		"direccion",
		"coordenadas_y",
		"coordenadas_x",
		"latitud",
		"longitud",
		"anho_cod_geo",
		"uri"
 	];

 	// Genera fila de encabezados tsv
 	var headerline = "";
   _.each(fields,function(f,i){
  	headerline+=f;
  	headerline+= i<(fields.length-1) ? "\t" : "\n";
  })  

  outData += headerline;
 
  _.each(data, function(d) {
    // Obtiene latitud y longitud de atributo de geom / "POINT (-56.1878813889 -24.5168777778)"
    if (d && d.geom) {
      var aux = d.geom.split("(")[1];
      var aux = aux.split(")")[0];
      var aux = aux.split(" ");
      var long = aux[0];
      var lat = aux[1];
      d.latitud=lat;
      d.longitud=long;
    } else {
    	d.latitud=null;
      d.longitud=null;
    }

    // genera linea condatos tsv
    var newline="";
    _.each(fields,function(f,i){
    	newline+=d[f];
    	newline+= i<(fields.length-1) ? "\t" : "\n";
    })    
    outData += newline




  })

  var fs = require('fs');
  fs.writeFile("./outfile.txt", outData, function(err) {
      if(err) {
          return console.log(err);
      }

      console.log("The file was saved!");
  }); 

  return deferred.promise;
}



utils.readDataFromFile(datafile)
.then(function(data) {
  console.log("Data read", data.length)
  return saveAsTXT(data)
})
.then(function(result) {
  console.log("END");
})
.catch(function(err) {
  console.log("ERROR",err)
})
.fin(function() {

})


//   


