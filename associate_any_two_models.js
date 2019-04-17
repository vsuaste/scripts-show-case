const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const axios = require('axios');

/**
* Example : Associate accesions to individual
* The file by user will looks like this:
*
* "name","origin","description","accessionId","field_unit_id","genotypeId","accession_idlocal","field_unit_idlocal","genotypeIdlocal"
* "1_1","Accession",NULL,NULL,NULL,NULL,"JSG-SRV-606","parcela_1",NULL
* "1_2","Accession",NULL,NULL,NULL,NULL,"JSG-SRV-606","parcela_1",NULL
* "1_3","Accession",NULL,NULL,NULL,NULL,"JSG-SRV-606","parcela_1",NULL
**/

// query name for obtaining all records of the model which record will be updated
let query_all_first = process.argv[2];  //"individuals";
// attribute by which the record to be updated will be found
let column_lookup_first = process.argv[3]; //"name";

// query name for obtaining all records of the model which record will be the associated one
let query_all_second = process.argv[4]; //"accessions";
// attribute by which the record will be found
let column_lookup_second = process.argv[5]; //"accession_id";
// name of the column in the temporary file that behaves as foreignKey
let column_local_association = process.argv[6];  //"accession_idlocal";

// query name to update a record
let update_query = process.argv[7]; //"updateIndividual";
// attribute name to be updated
let foreignKey = process.argv[8];  //"accessionId";

// temporary file to be processed
let input_file = process.argv[9];  //"associate_individuals_test.csv";
// server url
let server_url = process.argv[10];  //"http://localhost:3000/graphql";


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
  let query = `mutation { ${update_query}( id: ${id} ${foreignKey}: ${id_association}){ id  } }`
  return axios.post(server_url, {
    query: query
  }).then( response => {
    return response.data.data[ update_query ];
  }).catch( error => {
    console.log(id_association, " ERROR IN UPDATE");
    throw error.response;
  });
}

/**
 * This function perfoms the process to actually doing the association between two records by following the next steps:
 * 1.- Search id of record that will be updated (searching by an attribute that behaves as primaryKey for the user, this attribute is passed to the script as column_lookup_first)
 * 2.- Search id of record that will be associated to record found in step one.
 * 3.- Updated record found in step 1 with foreignKey equal to the id found in step 2.
 */
const updateAssociation = async () =>{
  await asyncForEach(results.data, async record_data =>{

    try{
      let first_record = await searchByLocalAttribute(record_data[column_local_association], query_all_first, column_lookup_first);
      let second_record = await searchByLocalAttribute(record_data[column_lookup_second], query_all_second, column_lookup_second);

      let sciencedb_id_first = first_record.id;
      let sciencedb_id_second =  second_record.id;
      let result = await updateById(sciencedb_id_first, sciencedb_id_second);


    }catch(error){
      console.log( error );
      return;
    }

  })
}

updateAssociation();
