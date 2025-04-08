import jsonld from "jsonld";
import { Parser, Store } from "n3";

import validateTriple from "./validateTriple";

export default class DataExtractor {
  async extract(jsonLdData) {
    try {
      const nquadsData = await jsonld.toRDF(jsonLdData, {
        format: "application/n-quads",
      });
      return validateTriples(nquadsData);
    } catch (error) {
      console.log("error fetching jsonLd", error);
    }
  }
}

async function validateTriples(nquadsData) {
  const validTriples = new Store();
  const invalidTriples = new Store();
  const originalTriples = new Store();

  const parser = new Parser({ format: "N-Triples" });

  //TODO: unsure if we really need the parser. We're unsure about the format of nquadsData...
  // This might be a lot of boilerplate for no reason.
  await new Promise((resolve, reject) => {
    parser.parse(nquadsData, async (error, quad) => {
      if (error) {
        console.error("Error parsing N-Triples:", error);
        reject(error);
      }
      if (quad) {
        originalTriples.addQuad(quad);
      }
      else {
        resolve(originalTriples);
      }
    });
  });

  // Validate
  for(const quad of originalTriples){
    if(validateTriple(quad)) {
      validTriples.addQuad(quad);
    }
    else {
      // Note: This is a bit weird, but let's say
      // we validate for triples compatible in our systems
      // which should be a subset of syntactically correct triples.
      invalidTriples.addQuad(quad);
    }
  }
  return { originalTriples, validTriples, invalidTriples };
}
