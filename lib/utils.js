import { Writer } from "n3";
import { updateSudo } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { chunk } from 'lodash';


/**
 * convert results of select query to an array of objects.
 * courtesy: Niels Vandekeybus & Felix
 * @method parseResult
 * @return {Array}
 */
export function parseResult( result ) {
  if(!(result.results && result.results.bindings.length)) return [];

  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => {
      console.log(row[key] && row[key].datatype);
      if(row[key] && row[key].datatype == 'http://www.w3.org/2001/XMLSchema#integer' && row[key].value){
        obj[key] = parseInt(row[key].value);
      }
      else if(row[key] && row[key].datatype == 'http://www.w3.org/2001/XMLSchema#dateTime' && row[key].value){
        obj[key] = new Date(row[key].value);
      }
      else obj[key] = row[key] ? row[key].value:undefined;
    });
    return obj;
  });
}

export async function storeToNTriples(store) {
  const serializer = new Writer({ format: "N-Triples" });
  for (const quad of store) {
    serializer.addQuad(quad);
  }
  return new Promise((resolve, reject) => {
    serializer.end((error, ntData) => {
      if (error) {
        reject(error);
      } else {
        resolve(ntData);
      }
    });
  });
}

export function storeAsArray(store) {
  const serializer = new Writer({ format: "N-Triples" });
  const quads = store.getQuads();
  const nTripleArray = [];

  for (const quad of quads) {
    const nTripleString = serializer.quadToString(quad.subject, quad.predicate, quad.object);
    nTripleArray.push(nTripleString);
  }

  return nTripleArray;
}

export async function batchedUpdate(
  nTriples,
  targetGraph,
  sleep,
  batch,
  extraHeaders,
  endpoint,
  operation) {
  console.log("size of store: ", nTriples?.length);
  const chunkedArray = chunk(nTriples, batch);
  let chunkCounter = 0;
  for (const chunkedTriple of chunkedArray) {
    console.log(`Processing chunk number ${chunkCounter} of ${chunkedArray.length} chunks.`);
    console.log(`using endpoint from utils ${endpoint}`);
    try {
      const updateQuery = `
        ${operation} DATA {
           GRAPH ${sparqlEscapeUri(targetGraph)} {
             ${chunkedTriple.join('')}
           }
        }
      `;
      console.log(`Hitting database ${endpoint} with batched query \n ${updateQuery}`);
      const connectOptions = { sparqlEndpoint: endpoint, mayRetry: true };
      console.log('connectOptions: ', connectOptions, "Extra headers: ", extraHeaders);
      await updateSudo(updateQuery, extraHeaders, connectOptions);
      console.log(`Sleeping before next query execution: ${sleep}`);
      await new Promise(r => setTimeout(r, sleep));

    }
    catch (err) {
      // Binary backoff recovery.
      console.log("ERROR: ", err);
      console.log(`Inserting the chunk failed for chunk size ${batch} and ${nTriples.length} triples`);
      const smallerBatch = Math.floor(batch / 2);
      if (smallerBatch === 0) {
        console.log("the triples that fails: ", nTriples);
        throw new Error(`Backoff mechanism stops in batched update,
          we can't work with chunks the size of ${smallerBatch}`);
      }
      console.log(`Let's try to ingest wiht chunk size of ${smallerBatch}`);
      await batchedUpdate(lib, chunkedTriple, targetGraph, sleep, smallerBatch, extraHeaders, endpoint, operation);
    }
    ++chunkCounter;
  }
}
