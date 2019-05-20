const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const axios = require('axios');

/**
* In this type of association, the created record can be no unique. The associated records will
* be added before uploading the table.
*
* Example : Associate measurement to individual/fieldUnit
* The file by user will looks like this:
*
* "name","short_name","method","reference","reference_link","value","unit","field_unit_id","individual_id","field_unit_idlocal","individual_idlocal"
* "Plant height","PH","Each individual was characterized according to the descriptions in Sáchez G. et al. (1998).","Sánchez, G., J. J., T. A. Kato Y., M. Aguilar S., J. M. Hernández C., A. López R., and J. A. Ruiz C. 1998. Distribución y Caracterización del Teocintle. Guadalajara, México: Centro de Investigación Regional del Pacífico Centro, Instituto Nacional de Investigaciones Forestales, Agrícolas y Pecuarias",NULL,168,"cm",NULL,NULL,"parcela_1","1_1"
* "Plant height","PH","Each individual was characterized according to the descriptions in Sáchez G. et al. (1998).","Sánchez, G., J. J., T. A. Kato Y., M. Aguilar S., J. M. Hernández C., A. López R., and J. A. Ruiz C. 1998. Distribución y Caracterización del Teocintle. Guadalajara, México: Centro de Investigación Regional del Pacífico Centro, Instituto Nacional de Investigaciones Forestales, Agrícolas y Pecuarias",NULL,230,"cm",NULL,NULL,"parcela_1","1_2"
*
**/

// name of file to process without .csv extension
let input_file_name = process.argv[2];  // 'teocointles_measurement_FM_output'

// query name for obtaining all records of the model that will be associated
let query_all_first =  process.argv[3]; //"fieldUnits"; // "individuals"; //

// attribute by which the record that will be associated will be found
let column_lookup_first = process.argv[4]; //"field_name"; //"name";//  //

// name of the column in the temporary file that behaves as foreignKey
let column_local_association = process.argv[5];  //"field_unit_idlocal"; //"individual_idlocal"; ////

// attribute name to be updated
let foreignKey = process.argv[6];  //"field_unit_id"; // "individual_id"; //

// temporary file to be processed
let input_file =  "../tablas_con_datos/"+ input_file_name + ".csv"; //

// server url
let server_url = process.argv[7];  //"http://localhost:3000/graphql";  //


let data = fs.readFileSync(path.join(__dirname, input_file), { encoding : 'utf8'});
let config = {
  header: true,
  dynamicTyping: true,
  skipEmptyLines: true,
  encoding: "UTF-8"
}
let results = Papa.parse(data, config);

async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}


/**
 * searchByLocalAttribute - Search record which attribute column_lookup is equal to local_attribute and obtain its id.
 *
 * @param  {String} local_attribute     Value by which the searching will be done
 * @param  {String} search_all_query Name of the query for searching all records
 * @param  {type} column_lookup       description
 * @return {type}                     description
 */
searchByLocalAttribute = function( local_attribute, search_all_query, column_lookup ){
  let query = `query { ${search_all_query}( search : {field: ${column_lookup} value:{ value: "${local_attribute}" }, operator: eq }  ){ id  } }`;
  return axios.post(server_url, {
    query: query
  }).then( response => {

    if(response.data.data[ search_all_query ].length === 0){
      throw new Error(local_attribute ," no found");
    }

    if(response.data.data[ search_all_query ].length > 1){
      return new Error(local_attribute ," no unique key");
    }
    return response.data.data[ search_all_query ][0];
  }).catch( error => {
    console.log(local_attribute, " ERROR IN SEARCH ");
    throw error.response;
  });
}

/**
 * This function perfoms the process to actually doing the association withing the file between two records by following the next steps:
 * 1.- Search id of record that will be associated (searching by an attribute that behaves as primaryKey for the user, this attribute is passed to the script as column_lookup_first)
 * 2.- Update the field 'foreignKey' in the file with the id found in step 1.
 * 3.- Delete from file the local attribute tha behaves as foreignKey.
 */
const updateAssociation = async () =>{
  await asyncForEach(results.data, async record_data =>{

    try{
      let first_record = await searchByLocalAttribute(record_data[column_local_association], query_all_first, column_lookup_first);
      //let second_record = await searchByLocalAttribute(record_data[column_local_association], query_all_second, column_lookup_second);

      let sciencedb_id_first = first_record.id;
        record_data[foreignKey ] = sciencedb_id_first;

        delete record_data[column_local_association];

    }catch(error){
      console.log( error );
      return;
    }

  })

  let updated_file = Papa.unparse(results.data);
  fs.writeFileSync("../tablas_con_datos/"+input_file_name + "_output.csv" ,updated_file, { encoding : 'utf8'})
  //console.log("FILE \n", updated_file);
}

updateAssociation();
