import { sparqlEscapeUri,  sparqlEscapeString, sparqlEscapeDateTime, uuid } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { TASK_TYPE,
         PREFIXES,
         STATUS_BUSY,
         STATUS_FAILED,
         ERROR_URI_PREFIX,
         TASK_HARVESTING_IMPORTING,
         ERROR_TYPE } from '../constants';
import { parseResult } from './utils';

export async function failBusyImportTasks() {
  const queryStr = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    DELETE {
      GRAPH ?g {
        ?task adms:status ${sparqlEscapeUri(STATUS_BUSY)} .
        ?task dct:modified ?modified.
      }
    }
    INSERT {
      GRAPH ?g {
       ?task adms:status ${sparqlEscapeUri(STATUS_FAILED)} .
       ?task dct:modified ${sparqlEscapeDateTime(new Date())}.
      }
    }
    WHERE {
      GRAPH ?g {
        ?task a ${ sparqlEscapeUri(TASK_TYPE) };
              adms:status ${sparqlEscapeUri(STATUS_BUSY)};
              task:operation ${sparqlEscapeUri(TASK_HARVESTING_IMPORTING)}.
        OPTIONAL { ?task dct:modified ?modified. }
      }
    }
   `;
  try {
    await update(queryStr);
  } catch(e) {
    console.warn(`WARNING: failed to move busy tasks to failed status on startup.`, e);
  }
}

export async function isTask( subject ){
  //TODO: move to ask query
  const queryStr = `
   ${PREFIXES}
   SELECT ?subject WHERE {
    GRAPH ?g {
      BIND(${ sparqlEscapeUri(subject) } as ?subject)
      ?subject a ${ sparqlEscapeUri(TASK_TYPE) }.
    }
   }
  `;
  const result = await query(queryStr);
  return result.results.bindings.length;
}

export async function loadExtractionTask( subject ){
  const queryTask = `
   ${PREFIXES}
   SELECT DISTINCT ?graph ?task ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
    GRAPH ?graph {
      BIND(${ sparqlEscapeUri(subject) } as ?task)
      ?task a ${ sparqlEscapeUri(TASK_TYPE) }.
      ?task dct:isPartOf ?job;
                    mu:uuid ?id;
                    dct:created ?created;
                    dct:modified ?modified;
                    adms:status ?status;
                    task:index ?index;
                    task:operation ${sparqlEscapeUri(TASK_HARVESTING_IMPORTING)}.

      OPTIONAL { ?task task:error ?error. }
    }
   }
  `;

  const task = parseResult(await query(queryTask))[0];
  if(!task) return null;

  //now fetch the hasMany. Easier to parse these
  const queryParentTasks = `
   ${PREFIXES}
   SELECT DISTINCT ?task ?parentTask WHERE {
     GRAPH ?g {
       BIND(${ sparqlEscapeUri(subject) } as ?task)
       ?task cogs:dependsOn ?parentTask.

      }
    }
  `;

  const parentTasks = parseResult(await query(queryParentTasks)).map(row => row.parentTask);
  task.parentSteps = parentTasks;

  const queryResultsContainers = `
   ${PREFIXES}
   SELECT DISTINCT ?task ?resultsContainer WHERE {
     GRAPH ?g {
       BIND(${ sparqlEscapeUri(subject) } as ?task)
       ?task task:resultsContainer ?resultsContainer.
      }
    }
  `;

  const resultsContainers = parseResult(await query(queryResultsContainers)).map(row => row.resultsContainer);
  task.resultsContainers = resultsContainers;

  const queryInputContainers = `
   ${PREFIXES}
   SELECT DISTINCT ?task ?inputContainer WHERE {
     GRAPH ?g {
       BIND(${ sparqlEscapeUri(subject) } as ?task)
       ?task task:inputContainer ?inputContainer.
      }
    }
  `;

  const inputContainers = parseResult(await query(queryInputContainers)).map(row => row.inputContainer);
  task.inputContainers = inputContainers;
  return task;
}

export async function updateTaskStatus(task, status){
  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified.
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${sparqlEscapeUri(status)}.
       ?subject dct:modified ${sparqlEscapeDateTime(new Date())}.
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${sparqlEscapeUri(task.task)} as ?subject)
        ?subject adms:status ?status .
        OPTIONAL { ?subject dct:modified ?modified. }
      }
    }
  `);
}

export async function appendTaskError(task, errorMsg){
  const id = uuid();
  const uri = ERROR_URI_PREFIX + id;

  const queryError = `
   ${PREFIXES}
   INSERT DATA {
    GRAPH ${sparqlEscapeUri(task.graph)}{
      ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(ERROR_TYPE)};
        mu:uuid ${sparqlEscapeString(id)};
        oslc:message ${sparqlEscapeString(errorMsg)}.
      ${sparqlEscapeUri(task.task)} task:error ${sparqlEscapeUri(uri)}.
    }
   }
  `;

  await update(queryError);
}
