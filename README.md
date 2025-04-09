# harvesting-import-service

Microservice that harvests knowledge about a harvesting-task from verenigingen-scraper
and writes the resulting triples to the database.

## Installation

To add the service to your stack, add the following snippet to docker-compose.yml:

```
services:
  harvesting-import:
    image: lblod/harvesting-verenigingen-import-service:x.x.x
    volumes:
      - ./data/files:/share
```

## Configuration

### Delta

```
{
  match: {
    predicate: {
      type: 'uri',
      value: 'http://www.w3.org/ns/adms#status'
    },
    object: {
      type: 'uri',
      value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'
    }
  },
  callback: {
    method: 'POST',
    url: 'http://harvesting-import/delta'
  },
  options: {
    resourceFormat: 'v0.0.1',
    gracePeriod: 1000,
    ignoreFromSelf: true
  }
}
```

This service will filter out <http://redpencil.data.gift/vocabularies/tasks/Task> with operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/importing>.

### Environment variables

#### `SLEEP_BETWEEN_IMPORT_BATCHES`

- **Description**: Specifies the delay in milliseconds between import batches. This delay helps manage the load on the system by spacing out the import operations.
- **Type**: Positive Integer
- **Example**: `100` (in milliseconds)
- **Default**: `1` millisecond

#### `SIZE_IMPORT_BATCHES`

- **Description**: Defines the size of each import batch. This determines how many items are processed in a single batch during the import operation.
- **Type**: Positive Integer
- **Example**: `1000`
- **Default**: `1000`

#### `ENDPOINT_IMPORT_BATCHES`

- **Description**: The URL endpoint for importing batches. This endpoint should point to the service that handles the batch import operations.
- **Type**: URL String
- **Example**: `http://virtuoso:8890/sparql`
- **Default**: `http://virtuoso:8890/sparql`

#### `IMPORT_TARGET_GRAPH`

- **Description**: The target graph URL where the imported data will be stored. This URL identifies the specific graph within the RDF store.
- **Type**: URL String
- **Example**: `http://mu.semte.ch/graphs/harvesting`
- **Default**: `http://mu.semte.ch/graphs/harvesting`

#### `ENDPOINT_REPLACE_SOURCE_GRAPH_OPERATION`

- **Description**: The URL endpoint for the operation that replaces the source graph. This endpoint is used to update the source graph with new data.
- **Type**: URL String
- **Example**: `http://virtuoso:8890/sparql`
- **Default**: `http://virtuoso:8890/sparql`

---

## Validation and correction

The service will validate the triples to import. (What defines a valid triple is context-sensitive here; it means valid and compatible with our system.)
Valid, invalid, and corrected triples are written to a file.

## REST API

### POST /delta

Starts the import of the given harvesting-tasks into the database.

- Returns `204 NO-CONTENT` if no harvesting-tasks could be extracted.
- Returns `200 SUCCESS` if the harvesting-tasks were successfully processed.
- Returns `500 INTERNAL SERVER ERROR` if something unexpected went wrong while processing the harvesting-tasks.

## Model

See [lblod/job-controller-service](https://github.com/lblod/job-controller-service)
