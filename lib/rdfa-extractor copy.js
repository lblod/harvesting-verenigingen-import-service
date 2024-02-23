import streamToArray from "stream-to-array";
import { RdfaParser } from "rdfa-streaming-parser";
import { createUnzip } from "zlib";
import jsonld from "jsonld";
import { createReadStream } from "fs";
import { DataFactory, Store, Parser, Writer } from "n3";
const { namedNode, quad } = DataFactory;
import { pipeline as pipelineAsync } from "stream/promises";
import { Writable } from "stream";

import validateTriple from "./validateTriple";
import fixTriple from "./fixTriple";

//todo this can be removed?
// class SourceAwareStoreWriter extends Writable {
//   constructor(url, store) {
//     super({ objectMode: true });
//     this.subjects = [];
//     this.store = store;
//     this.url = namedNode(url);
//   }

//   _write(data, _enc, next) {
//     this.store.addQuad(data);
//     if (!this.subjects.includes(data.subject.value)) {
//       this.subjects.push(data.subject.value);
//       this.store.addQuad(
//         quad(
//           data.subject,
//           namedNode("http://www.w3.org/ns/prov#wasDerivedFrom"),
//           this.url
//         )
//       );
//     }
//     return next();
//   }
// }
export default class RDFAextractor {
  //todo this can be removed?
  // async extractPage(filePath, fileMetadata) {
  //   const store = new Store();
  //   const parser = new RdfaParser({ baseIRI: fileMetadata.url });
  //   const writer = new SourceAwareStoreWriter(fileMetadata.url, store);
  //   try {
  //     if (filePath.endsWith(".gz")) {
  //       await pipelineAsync(
  //         createReadStream(filePath),
  //         createUnzip(),
  //         parser,
  //         writer
  //       );
  //     } else {
  //       await pipelineAsync(createReadStream(filePath), parser, writer);
  //     }
  //     return store;
  //   } catch (e) {
  //     console.error(`ERROR extracting file with path ${filePath}`, e);
  //     throw e;
  //   }
  // }

  async ttl(url) {
    const response = await fetch(url);
    const jsonLdData = await response.json();
    const fetchContext = await fetch(jsonLdData["@context"]);
    const jsonLdContext = await fetchContext.json();
    jsonLdData["@context"] = jsonLdContext;
    const nquadsData = await jsonld.toRDF(jsonLdData, {
      format: "application/nquads",
    });
    const ntriplesData = convertNquadsToNtriples(nquadsData);
    return ntriplesData;
  }
}
async function convertNquadsToNtriples(nquadsData) {
  console.log(nquadsData);
  const parser = new Parser();
  const quadData = parser.parse(nquadsData);
  const serializer = new Writer({ format: "N-Triples" });
  quadData.forEach((quad) => {
    serializer.addQuad(quad);
  });

  return new Promise((resolve, reject) => {
    serializer.end(async (error, ntData) => {
      if (error) {
        console.error("Error serializing N-Triples:", error);
        reject(error);
      } else {
        const originalTriples = [];
        const parser = new Parser();
        parser.parse(ntData, (parseError, triple) => {
          if (parseError) {
            console.error("Error parsing N-Triples:", parseError);
            reject(parseError);
          } else if (triple) {
            if (triple.subject.termType === "BlankNode") {
              const replacementValue = "http://example.com/replacement";
              const replacedTriple = {
                subject: { termType: "NamedNode", value: replacementValue },
                predicate: triple.predicate,
                object: triple.object,
              };
              originalTriples.push(replacedTriple);
            } else {
              originalTriples.push(triple);
            }
          } else {
            async function validateAndSendTriples() {
              const { validTriples, invalidTriples, correctedTriples } =
                await correctAndRepairTriples(originalTriples);
              const result = {
                validTriples: await convertTriplesToNtriples(validTriples),
                invalidTriples: await convertTriplesToNtriples(invalidTriples),
                correctedTriples: await convertTriplesToNtriples(
                  correctedTriples
                ),
                originalTriples: ntData,
              };
              resolve(result);
            }
            validateAndSendTriples();
          }
        });
      }
    });
  });
}

async function convertTriplesToNtriples(triples) {
  const serializer = new Writer({ format: "N-Triples" });
  for (const triple of triples) {
    serializer.addQuad(triple);
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

async function correctAndRepairTriples(triples) {
  const validTriples = [];
  const invalidTriples = [];
  const correctedTriples = [];

  for (const triple of triples) {
    if (await validateTriple(triple)) {
      validTriples.push(triple);
    } else {
      invalidTriples.push(triple);
    }
  }

  // for (const triple of invalidTriples) {
  //   const fixedTriple = await fixTriple(triple);
  //   if (fixedTriple) {
  //     validTriples.push(fixedTriple);
  //     correctedTriples.push(triple);
  //   }
  // }
  return { validTriples, invalidTriples, correctedTriples };
}
