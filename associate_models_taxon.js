const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const axios = require('axios');

let server_url = "http://localhost:3000/graphql"
let column_lookup = "accession_id";
let column_tofill = "taxon_id";
let search_all_resolver = "accessions";
let update_resolver = "updateAccession";
let input_file = "test_association.csv";

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


searchByLocalAttribute = function( local_attribute ){
  let query = `query { ${search_all_resolver}( search : {field: ${column_lookup} value:{ value: "${local_attribute}" }, operator: eq }  ){ id  } }`;
  return axios.post(server_url, {
    query: query
  }).then( response => {

    if(response.data.data[ search_all_resolver ].length > 1){
      return new Error(local_attribute ," no unique key");
    }
    return response.data.data[ search_all_resolver ][0];
  }).catch( error => {
    console.log(local_attribute, " ERROR IN SEARCH ");
    throw error.response;
  });
}

updateById = function(id, id_association){
  let query = `mutation { ${update_resolver}( id: ${id} ${column_tofill}: "${id_association}"){ id ${column_tofill} } }`
  return axios.post(server_url, {
    query: query
  }).then( response => {
    return response.data.data[ update_resolver ];
  }).catch( error => {
    console.log(id_association, " ERROR IN UPDATE");
    throw error.response;
  });
}

const updateAssociation = async () =>{
  await asyncForEach(results.data, async ids_data =>{

    try{
      let record_data = await searchByLocalAttribute( ids_data[ column_lookup ] );
      let sciencedb_id = record_data.id;

      let updated_record = await updateById(sciencedb_id, ids_data[column_tofill ] );

    }catch(error){
      console.log( error );
      return;
    }

  })
}


updateAssociation();
