import { sparqlEscapeUri, uuid } from "mu";
import { v5 as uuidv5 } from "uuid";
import * as N3 from 'n3';
import * as rst from 'rdf-string-ttl';
const { namedNode, literal } = N3.DataFactory;
import { updateSudo as update } from "@lblod/mu-auth-sudo";
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
} from "../constants";

import {
  IMPORT_TARGET_GRAPH,
  SIZE_IMPORT_BATCHES_INCREMENTAL,
  ENDPOINT_IMPORT_BATCHES
} from '../config';

import { storeToNTriples, storeAsArray, updateMutatiedienstStateInfo } from './utils';

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

import { chunk } from 'lodash';

export async function run(task) {

  try {
    await updateTaskStatus(task, STATUS_BUSY);

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

    const verenigingenSubjects = validTriples
          .getSubjects(namedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                       namedNode("https://data.vlaanderen.be/ns/FeitelijkeVerenigingen#Vereniging"));

    const chunkedVerenigingenSubjects = chunk(verenigingenSubjects, SIZE_IMPORT_BATCHES_INCREMENTAL)

    for(const verenigingSubjectsChunk of chunkedVerenigingenSubjects) {
      await Promise.all(
        verenigingSubjectsChunk.map(async (verenigingSubject) => {
          const verenigingStore = extractVerenigingTriples(verenigingSubject, validTriples);
          await updateSourceGraph(IMPORT_TARGET_GRAPH, verenigingSubject, storeAsArray(verenigingStore));
        }));
    }

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

    await updateMutatiedienstStateInfo(validTriples)
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

function extractVerenigingTriples(verenigingSubject, validTriples) {

  let verenigingStore = new N3.Store();

  verenigingStore.addQuads(validTriples.getQuads(verenigingSubject));

  const identifierSubjects = validTriples.getObjects(
    verenigingSubject,
    namedNode("http://www.w3.org/ns/adms#identifier"));

  for(const identifierSubject of identifierSubjects) {
    verenigingStore.addQuads(validTriples.getQuads(identifierSubject));
    const gestructureerdeIdSubjects = validTriples.getObjects(
      identifierSubject,
      namedNode("https://data.vlaanderen.be/ns/generiek#gestructureerdeIdentificator")
    )
    for(const gestructureerdeIdSubject of gestructureerdeIdSubjects)
      verenigingStore.addQuads(validTriples.getQuads(gestructureerdeIdSubject));
  }

  const targetAudienceSubjects = validTriples.getObjects(
    verenigingSubject,
    namedNode("http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/doelgroep"));

  for(const targetAudienceSubject of targetAudienceSubjects)
    verenigingStore.addQuads(validTriples.getQuads(targetAudienceSubject));

  const contactpointSubjects = validTriples.getObjects(
    verenigingSubject,
    namedNode("http://schema.org/contactPoint"));

  for(const contactpointSubject of contactpointSubjects)
    verenigingStore.addQuads(validTriples.getQuads(contactpointSubject));

  const siteSubjects = [
    ...validTriples.getObjects(verenigingSubject, namedNode("http://www.w3.org/ns/org#hasSite")),
    ...validTriples.getObjects(verenigingSubject, namedNode("http://www.w3.org/ns/org#hasPrimarySite"))
  ];

  for(const siteSubject of siteSubjects) {
      verenigingStore.addQuads(validTriples.getQuads(siteSubject));
    const addressSubjects = validTriples.getObjects(
      siteSubject,
      namedNode("https://data.vlaanderen.be/ns/organisatie#bestaatUit")
    )
    for(const addressSubject of addressSubjects)
      verenigingStore.addQuads(validTriples.getQuads(addressSubject));
  }

  const memberSubjects = validTriples.getObjects(
    verenigingSubject,
    namedNode("http://www.w3.org/ns/org#hasMembership"));

  for(const memberSubject of memberSubjects) {
    verenigingStore.addQuads(validTriples.getQuads(memberSubject));
    const personSubjects = validTriples.getObjects(
      memberSubject,
      namedNode("http://www.w3.org/ns/org#member")
    )

    for(const personSubject of personSubjects) {
      verenigingStore.addQuads(validTriples.getQuads(personSubject));
      const contactSubjects = validTriples.getObjects(
        personSubject,
        namedNode("http://schema.org/contactPoint")
      )
      for(const contactSubject of contactSubjects)
        verenigingStore.addQuads(validTriples.getQuads(contactSubject));
    }
  }
  return verenigingStore;
}

/**
 * Updates the source graph with the newly fetched vereniging data
 * Note: If verenginigen are completely removed in the verenigingenregister, then these will be delete
 *   in the non-incremental job
 */
async function updateSourceGraph(sourceGraph, verenigingenSubject, updatedTriples) {
  const queryStr = `
    DELETE {
      GRAPH ${sparqlEscapeUri(sourceGraph)} {
        ?vereniging ?vp ?vo.
        ?id ?idp ?ido.
        ?gi ?gip ?gio.
        ?ta ?tap ?tao.
        ?cp ?cpp ?cpo.
        ?s ?sp ?so.
        ?a ?ap ?ao.
        ?m ?mp ?mo.
        ?mper ?mperp ?mpero.
        ?mpcp ?mpcpp ?mpcpo.
      }
    }
    WHERE {
      VALUES ?vereniging {
        ${rst.termToString(verenigingenSubject)}
      }
      GRAPH ${sparqlEscapeUri(sourceGraph)} {
        {
          ?vereniging ?vp ?vo.
        }
        UNION {
          ?vereniging <http://www.w3.org/ns/adms#identifier> ?id.
          ?id ?idp ?ido.
        }
        UNION {
          ?vereniging <http://www.w3.org/ns/adms#identifier> ?id.
          ?id <https://data.vlaanderen.be/ns/generiek#gestructureerdeIdentificator> ?gi.
          ?gi ?gip ?gio.
        }
        UNION {
          ?vereniging <http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/doelgroep> ?ta.
          ?ta ?tap ?tao.
        }
        UNION {
          ?vereniging <http://schema.org/contactPoint> ?cp.
          ?cp ?cpp ?cpo.
        }
        UNION {
          ?vereniging <http://www.w3.org/ns/org#hasSite>|<http://www.w3.org/ns/org#hasPrimarySite> ?s.
          ?s ?sp ?so.
        }
        UNION {
          ?vereniging <http://www.w3.org/ns/org#hasSite>|<http://www.w3.org/ns/org#hasPrimarySite> ?s.
          ?s <https://data.vlaanderen.be/ns/organisatie#bestaatUit> ?a.
          ?a ?ap ?ao.
        }
        UNION {
          ?vereniging <http://www.w3.org/ns/org#hasMembership> ?m.
          ?m ?mp ?mo.
        }
        UNION {
          ?vereniging <http://www.w3.org/ns/org#hasMembership> ?m.
          ?m <http://www.w3.org/ns/org#member> ?mper.
          ?mper ?mperp ?mpero.
        }
        UNION {
          ?vereniging <http://www.w3.org/ns/org#hasMembership> ?m.
          ?m <http://www.w3.org/ns/org#member> ?mp.
          ?mp <http://schema.org/contactPoint> ?mpcp.
          ?mpcp ?mpcpp ?mpcpo.
        }
      }
    }

    ;

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(sourceGraph)} {
        ${updatedTriples.join('\n')}
      }
    }
  `;

  await update(queryStr, {}, { sparqlEndpoint: ENDPOINT_IMPORT_BATCHES, mayRetry: true });
}
