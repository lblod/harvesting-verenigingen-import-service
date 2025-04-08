import envvar from 'env-var';

export const SLEEP_BETWEEN_IMPORT_BATCHES = envvar
  .get('SLEEP_BETWEEN_IMPORT_BATCHES')
  .example('100 (in ms)')
  .default(1)
  .asIntPositive();

export const SIZE_IMPORT_BATCHES = envvar
  .get('SIZE_IMPORT_BATCHES')
  .example('1000')
  .default(1000)
  .asIntPositive();

export const ENDPOINT_IMPORT_BATCHES = envvar
  .get('ENDPOINT_IMPORT_BATCHES')
  .example('http://virtuoso:8890/sparql')
  .default('http://virtuoso:8890/sparql')
  .asUrlString();

export const IMPORT_TARGET_GRAPH = envvar
  .get('IMPORT_TARGET_GRAPH')
  .example('http://mu.semte.ch/graphs/harvesting')
  .default('http://mu.semte.ch/graphs/harvesting')
  .asUrlString();

export const ENDPOINT_REPLACE_SOURCE_GRAPH_OPERATION = envvar
  .get('ENDPOINT_REPLACE_SOURCE_GRAPH_OPERATION')
  .example('http://virtuoso:8890/sparql')
  .default('http://virtuoso:8890/sparql')
  .asUrlString();
