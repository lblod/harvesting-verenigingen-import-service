import jsonld from "jsonld";
import { DataFactory, Parser, Writer, Store } from "n3";
const { namedNode, literal } = DataFactory;

import validateTriple from "./validateTriple";
export default class RDFAextractor {
  async ttl(url) {
    const response = await fetch(url);
    const jsonLdData = await response.json();
    const nquadsData = await jsonld.toRDF(jsonLdData, {
      format: "application/n-quads",
    });
    return convertNquadsToNtriples(nquadsData);
  }
}

async function convertNquadsToNtriples(nquadsData) {
  const blankNodePrefix = "http://example.com/subject/";
  const store = new Store();
  const parser = new Parser({ format: "N-Triples", blankNodePrefix });
  const writer = new Writer({ format: "N-Triples" });
  const validTriples = [];
  const invalidTriples = [];
  const correctedTriples = [];
  const data = await new Promise((resolve, reject) => {
    parser.parse(nquadsData, async (error, quad) => {
      if (error) {
        console.error("Error parsing N-Triples:", error);
        reject(error);
      }
      if (quad) {
        const { subject, predicate, object } = quad;
        const triple = DataFactory.triple(
          subject.value.includes("example.com")
            ? namedNode(subject.value.replace("_:"))
            : namedNode(subject.value),
          namedNode(predicate.value.replace("@", "")),
          object.value.includes("example.com")
            ? namedNode(object.value.replace("_:"))
            : object
        );
        store.addQuad(quad);
        if (await validateTriple(triple)) {
          validTriples.push(triple);
        } else {
          invalidTriples.push(triple);
        }
      } else {
        writer.addQuads(store.getQuads());
        writer.end((error, originalTriples) => {
          if (error) {
            console.error("Error serializing N-Triples:", error);
            res.status(500).json({ error: "Internal Server Error" });
            reject(error);
          } else {
            resolve(originalTriples);
          }
        });
      }
    });
  });
  return {
    validTriples: await convertTriplesToNtriples(validTriples),
    invalidTriples: await convertTriplesToNtriples(invalidTriples),
    correctedTriples: await convertTriplesToNtriples(correctedTriples),
    originalTriples: await data,
  };
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
