import { sparqlEscapeUri, sparqlEscapeString, uuid } from "mu";
import { v5 as uuidv5 } from "uuid";
import * as N3 from 'n3';
const { namedNode, literal } = N3.DataFactory;
import { createReadStream } from "fs";
import zlib from "zlib";
import { querySudo as query, updateSudo as update } from "@lblod/mu-auth-sudo";
import {
  PREFIXES,
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
} from "../constants";

import {
  SLEEP_BETWEEN_IMPORT_BATCHES,
  SIZE_IMPORT_BATCHES,
  ENDPOINT_IMPORT_BATCHES,
  IMPORT_TARGET_GRAPH,
  ENDPOINT_REPLACE_SOURCE_GRAPH_OPERATION
} from '../config';

import { storeToNTriples, batchedUpdate, storeAsArray } from './utils';

import DataExtractor from "./data-extractor";
import {
  getFilePath,
  writeTtlFile,
  appendTempFile,
} from "./file-helpers";
import { loadExtractionTask, updateTaskStatus, appendTaskError } from "./task";


export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  if (!task) return;

  try {
    updateTaskStatus(task, STATUS_BUSY);

    let { jsonData, fileName } = await getCollectedDataFromTask(task);
    const extractor = new DataExtractor();

    const{ originalTriples, validTriples, invalidTriples } = await extractor.extract(jsonData);

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
    const queryStr = `
      CLEAR GRAPH ${sparqlEscapeUri(sourceGraph)};
      MOVE ${sparqlEscapeUri(tempImportGraph)} TO ${sparqlEscapeUri(sourceGraph)};
      CLEAR GRAPH ${sparqlEscapeUri(tempImportGraph)}
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

     await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

async function getCollectedDataFromTask(task) {
  const result = await query(`
  ${PREFIXES}
  SELECT DISTINCT ?file ?fileName
  WHERE {
     GRAPH ?g {
        ${sparqlEscapeUri(task.task)} task:inputContainer ?container.
        ?container task:hasFile ?file.
        ?file <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileName> ?fileName .
     }
  }
  `);
  if (result.results.bindings.length) {
    const fileName = result.results.bindings.map(
      (binding) => binding["file"].value
    )[0];
    const filePath = await getFilePath(fileName);
    const gunzip = zlib.createGunzip();
    const fileContent = createReadStream(filePath);
    let jsonData = "";
    return new Promise((resolve, reject) => {
      fileContent
        .pipe(gunzip)
        .on("data", function (data) {
          jsonData += data;
        })
        .on("end", function () {
          resolve({jsonData: JSON.parse(jsonData), fileName});
          console.log("File unzipped and parsed successfully.");
        })
        .on("error", function (err) {
          reject(err);
        });
    });
  } else {
    return [];
  }
}

async function appendTaskResultFile(task, container, fileUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(
    container.id
  )}.
        ${sparqlEscapeUri(container.uri)} task:hasFile ${sparqlEscapeUri(
    fileUri
  )}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(
    container.uri
  )}.
      }
    }
  `;

  await update(queryStr);
}

async function appendTaskResultGraph(task, container, graphUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(
    container.id
  )}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(
    graphUri
  )}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(
    container.uri
  )}.
      }
    }
  `;

  await update(queryStr);
}
