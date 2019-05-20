const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const axios = require('axios');

/**
* In this type of association, the created record can be no unique.
* The association is of the type one-to-manay or many-to-many
*
* Example : Associate sample to individual
* In this example is expected that the name in the sample is not unique
* The file by user will looks like this:
*
* "name","sample_type","tissue","compound","biological_replicate_num","treatment","pool_dna_extracts","sample_vol_weight","individualIdlocal"
* "BIXC_100_10_0","tissue","young leaf","DNA",0,NULL,FALSE,NULL,"100_10"
* "BIXC_100_11_0","tissue","young leaf","DNA",0,NULL,FALSE,NULL,"100_11"
* "BIXC_100_12_0","tissue","young leaf","DNA",0,NULL,FALSE,NULL,"100_12"
**/

// query name to create a record
let create_query = process.argv[2]; //"addSample";

// query name to update a record
let update_query = process.argv[3]; // "updateSample"; //

//resolver name for adding the associated records
let addResolver = process.argv[4];  //"addIndividuals";

// query name for obtaining all records of the model that wiill be associated
let query_all_first = process.argv[5];  //"individuals"; //

// attribute by which the record will be found
let column_lookup_first = process.argv[6]; //"name"; //

// name of the column in the temporary file that behaves as foreignKey
let column_local_association = process.argv[7];  //"individualIdlocal"; //

// temporary file to be processed
let input_file = process.argv[8];  // "../local-show-case-data/teocintles_sample.csv"; //
// server url
let server_url = process.argv[9];  //"http://localhost:3000/graphql";  //


let dictionary_queries = {
  "addSample": `mutation addSample(
   $name:String  $sampling_day:Int  $sampling_month:Int  $sampling_year:Int  $sample_type:String  $tissue:String  $compound:String  $biological_replicate_num:Int  $treatment:String  $pool_dna_extracts:Boolean  $sample_vol_weight:String      $addSamples:[ID] $addSequencing_experiments:[ID] $addPlates:[ID] $addLibrary_results:[ID] $addIndividuals:[ID] $addGenotyping_data:[ID]  ){
    addSample(
     name:$name   sampling_day:$sampling_day   sampling_month:$sampling_month   sampling_year:$sampling_year   sample_type:$sample_type   tissue:$tissue   compound:$compound   biological_replicate_num:$biological_replicate_num   treatment:$treatment   pool_dna_extracts:$pool_dna_extracts   sample_vol_weight:$sample_vol_weight           addSamples:$addSamples  addSequencing_experiments:$addSequencing_experiments  addPlates:$addPlates  addLibrary_results:$addLibrary_results  addIndividuals:$addIndividuals  addGenotyping_data:$addGenotyping_data     ){id}
  }`,

  "addPlate": ` mutation addPlate(
   $plate_id:String  $type:String  $row:String  $column:Int  $well:String  $barcode:String  $storage:String  $concentration:Float  $concentration_measurement_method:String  $library_prep_id:String  $well_id:String    $experiment_id:Int    $addSamples:[ID]  ){
    addPlate(
     plate_id:$plate_id   type:$type   row:$row   column:$column   well:$well   barcode:$barcode   storage:$storage   concentration:$concentration   concentration_measurement_method:$concentration_measurement_method   library_prep_id:$library_prep_id   well_id:$well_id       experiment_id:$experiment_id      addSamples:$addSamples     ){id  plate_id   type   row   column   well   barcode   storage   concentration   concentration_measurement_method   library_prep_id   well_id   }
  }
  `


}



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
 * updateById - Updates the attribute foreignKey with id_association
 * for record with id = id
 *
 * @param  {Int} id             Id of the record to update.
 * @param  {Int} id_association Id of the foreignKey that will be associated.
 * @return {String}                If update was successfull it will return a string sayin so.
 */
updateById = function(id, id_association){
  let query = `mutation { ${update_query}( id: ${id} ${addResolver}: [${id_association} ]){ id  } }`
  return axios.post(server_url, {
    query: query
  }).then( response => {
    return response.data.data[ update_query ];
  }).catch( error => {
    console.log(id_association, " ERROR IN UPDATE");
    throw error.response;
  });
}

createAndAssociate = function(record){
  let query = dictionary_queries[ create_query ];

  console.log("RECORD",record);

  for(let key in record){
    if(record[key ] === 'NULL'){
      delete record[key];
    }
  }


  return axios.post(server_url, {
    query: query,
    variables: record
  }).then( response => {
   return response.data.data[ create_query ];
  }).catch( error => {
    console.log(record.name, " ERROR IN CREATE-ASSOCIATE");
    //console.log(error.data);
    throw error.response;
  });
}


/**
 * This function perfoms the process to actually doing the association between two records by following the next steps:
 * 1.- Create record with the given data from file. This will use the create query name given by user.
 * 2.- Search id of record that will be associated to record created in step one.
 * 3.- Updated record created in step 1 with foreignKey equal to the id found in step 2.
 */
const updateAssociation = async () =>{
  await asyncForEach(results.data, async record_data =>{

    try{
      let first_record = await createAndAssociate(record_data);
      let second_record = await searchByLocalAttribute(record_data[column_local_association], query_all_first, column_lookup_first);

      let sciencedb_id_first = first_record.id;

      let sciencedb_id_second =  second_record.id;
      let result = await updateById(sciencedb_id_first, sciencedb_id_second);


    }catch(error){
      console.log(error);
      console.log( error.data );
      return;
    }

  })
}

updateAssociation();
