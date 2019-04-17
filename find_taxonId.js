const fs = require('fs');
const csvjson = require('csvjson');
const path = require('path');
const axios = require('axios');
const Papa = require('papaparse');

let conabio_service_url = process.argv[2];
let column_lookup = "accession_id";
let column_tofill = "taxon_id";
let output_file = "accesion_taxon_ids.csv";
fs.appendFileSync( output_file, column_lookup+","+ column_tofill+ "\n", err =>{
  if(err) throw err;
});


let data = fs.readFileSync(path.join(__dirname, 'teocintles_accession.csv'), { encoding : 'utf8'});
let config = {
  header: true,
  dynamicTyping: true,
  skipEmptyLines: true,
  encoding: "UTF-8"
}
let results = Papa.parse(data, config);

let tree = ['género', 'especie', 'subespecie', 'raza'];
getTaxonId = function( record, stop){

  let string_record = "";
  for(let i=0; i<stop; i++){
    if(i == 2){
      string_record+= ' subsp.'
    }
    string_record+= ' ' + record[ tree[i] ].trim();
  }

  string_record = string_record.trim();
//  let record_tree = tree.map( item =>{ return record[ item ].trim() })

  console.log(string_record);
  return axios.post(conabio_service_url, {
    query: `query {searchTaxon(query: "${string_record}"){totalCount edges{ node{ id scientificName taxonRank }} } }`
  }).then( response => {
    console.log(response.data.data.searchTaxon);
    return response.data.data.searchTaxon;
  }).catch(error =>{
    console.log(error.response.data.searchTaxon);
    throw error;
  })
  //let query = record[ tree[0] ]+ ' ' + record[tree[1]] + ''

}

async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}


const processCsvData = async () => {
  await asyncForEach( results.data, async record =>  {
    if(record['género']=== "" || record['género'] === "NULL"){
      console.log('ERROR : You must specifie at least genero for the record ', record);
      return;
    }
    let found = false;
    for(let i = 4; i>0; i--){
      if(found){ console.log("FOUND SHOULD RETURN "); return; }

      let response = await getTaxonId(record, i);
      if(response.totalCount == 1){
        record[ column_tofill ] = response.edges[0].node.id;
        fs.appendFileSync( output_file, '\"' +record[column_lookup]+ '\"' +","+ '\"'+record[column_tofill]+'\"'+ "\n", err =>{
          if(err) throw err;
        });
        found = true;
      }
    }

    console.log('ERROR: We could not find a unique ', column_tofill, " for this record", record);

  });

  console.log(results);
}

processCsvData();
