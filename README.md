# harvesting-import-service

Microservice that harvests knowledge about a harvesting-task from the linked annotated documents
and writes the resulting triples to the database.

## Installation

To add the service to your stack, add the following snippet to docker-compose.yml:

```
services:
  harvesting-import:
    image: lblod/harvesting-import-service:x.x.x
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
  },
```
This service will filter out  <http://redpencil.data.gift/vocabularies/tasks/Task> with operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/importing>.

### Environment variables

 - TARGET_GRAPH: refers to the graph where the harvested triples will be imported into.
 Defaults to <http://mu.semte.ch/graphs/public>.

## Validation and correction
The service will lis
The service will validate the triples to import and will try its best to correct the ones that it founds invalid.
Valid, invalid and corrected triples are written to a file.

## REST API

### POST /delta

Starts the import of the given harvesting-tasks into the db

- Returns `204 NO-CONTENT` if no harvesting-tasks could be extracted.

- Returns `200 SUCCESS` if the harvesting-tasks where successfully processes.

- Returns `500 INTERNAL SERVER ERROR` if something unexpected went wrong while processing the harvesting-tasks.


## Model
See [lblod/job-controller-service](https://github.com/lblod/job-controller-service)
