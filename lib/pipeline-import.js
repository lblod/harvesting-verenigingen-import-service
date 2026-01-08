import { sparqlEscapeUri, uuid } from "mu";
import { v5 as uuidv5 } from "uuid";
import * as N3 from 'n3';
const { namedNode, literal } = N3.DataFactory;
import { updateSudo as update } from "@lblod/mu-auth-sudo";
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
} from "../constants";
import {
  SLEEP_BETWEEN_IMPORT_BATCHES,
  SIZE_IMPORT_BATCHES,
  ENDPOINT_IMPORT_BATCHES,
  IMPORT_TARGET_GRAPH,
  ENDPOINT_REPLACE_SOURCE_GRAPH_OPERATION,
  FEATURE_ENABLE_MOVE_AUTOCOMMIT
} from '../config';

import { storeToNTriples, batchedUpdate, storeAsArray, updateMutatiedienstStateInfo } from './utils';

import DataExtractor from "./data-extractor";

import {
  writeTtlFile,
  appendTempFile,
} from "./file-helpers";

import { updateTaskStatus,
         appendTaskError,
         getCollectedDataFromTask,
         appendTaskResultFile
       } from "./task";


export async function run(task) {

  try {
    await updateTaskStatus(task, STATUS_BUSY);

    let { jsonData, fileName } = await getCollectedDataFromTask(task);
    const extractor = new DataExtractor();

    const{ originalTriples, validTriples, invalidTriples } = await extractor.extract(jsonData);

    const stateInfoStore = helpMoveStateInfoFromLocalStoreToNewStore(validTriples);

    // Add uuids
    for(const subject of validTriples.getSubjects()) {
      validTriples.addQuad(subject,
                           namedNode("http://mu.semte.ch/vocabularies/core/uuid"),
                           literal(uuidv5(subject.id, uuidv5.URL))
                          );
    }

    const validNTriplesAsArray = storeAsArray(validTriples);
    const tempImportGraph = `http://mu.semte.ch/graphs/harvesting/tasks/import/${uuid()}`;
    await batchedUpdate(validNTriplesAsArray,
                        tempImportGraph,
                        SLEEP_BETWEEN_IMPORT_BATCHES,
                        SIZE_IMPORT_BATCHES,
                        {},
                        ENDPOINT_IMPORT_BATCHES,
                        'INSERT');

    const sourceGraph = IMPORT_TARGET_GRAPH;

    //Note: this is only tested in virtuoso
    // 3: Autocommit mode with logging - i.e. row level commits - avoids transaction log size limit
    // !! Non atomic - to be used with caution, need to ensure healing doesn't run in parallel !!
    const logEnableDirective = FEATURE_ENABLE_MOVE_AUTOCOMMIT ? 'DEFINE sql:log-enable 3' : '';
    const queryStr = `
      ${logEnableDirective}
      MOVE ${sparqlEscapeUri(tempImportGraph)} TO ${sparqlEscapeUri(sourceGraph)}
    `;

    const connectOptions = { sparqlEndpoint: ENDPOINT_REPLACE_SOURCE_GRAPH_OPERATION, mayRetry: true };
    console.log('connectOptions: ', connectOptions);
    await update(queryStr, {}, connectOptions);

    // Note: for debugging purposes; we'll keep this information and link it to the task
    // The effective operation in the database, will be performed on 'validTriples'
    const orginalTempFilePath = `/share/original-${uuid()}.ttl`;
    const validTempFilePath = `/share/valid-triples-${uuid()}.ttl`;
    const invalidTempFilePath = `/share/invalid-triples-${uuid()}.ttl`;

    await appendTempFile(await storeToNTriples(originalTriples), orginalTempFilePath);
    await appendTempFile(await storeToNTriples(validTriples), validTempFilePath);
    await appendTempFile(await storeToNTriples(invalidTriples), invalidTempFilePath);

    const orginalFileUri = await writeTtlFile(
      task.graph,
      orginalTempFilePath,
      "original.ttl",
      fileName
    );

    const validFile = await writeTtlFile(
      task.graph,
      validTempFilePath,
      "valid-triples.ttl",
      fileName
    );

    const inValidFile = await writeTtlFile(
      task.graph,
      invalidTempFilePath,
      "invalid-triples.ttl",
      fileName
    );

    const fileContainer = { id: uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;

    await appendTaskResultFile(task, fileContainer, orginalFileUri);
    await appendTaskResultFile(task, fileContainer, validFile);
    await appendTaskResultFile(task, fileContainer, inValidFile);

    await updateMutatiedienstStateInfo(stateInfoStore);
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

function helpMoveStateInfoFromLocalStoreToNewStore(store) {
  let newStore = new N3.Store();
  const stateInfos = store.getQuads(
    undefined,
    namedNode("http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/lastSequenceMutatiedienst")
  );
  newStore.addQuads(stateInfos);
  store.removeQuads(stateInfos);
  return newStore;
}
