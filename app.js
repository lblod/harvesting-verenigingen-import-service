import {app, errorHandler} from 'mu';

import bodyParser from 'body-parser';

import { Delta } from "./lib/delta";
import {
  loadExtractionTask,
  failBusyImportTasks,
} from "./lib/task";
import {
  STATUS_SCHEDULED,
  TASK_HARVESTING_IMPORTING,
  TASK_HARVESTING_INCREMENTAL_IMPORTING
} from './constants';

import {
  run as runFullImport
} from './lib/pipeline-import';

import {
  run as runIncrementatlImport
} from './lib/pipeline-incremental-import';

/*
 * fail existing import tasks when (re)starting
 * this should clean up any task in a busy state after an unexpected service restart
 */
failBusyImportTasks();

app.use(bodyParser.json({
  type: function (req) {
    return /^application\/json/.test(req.get('content-type'));
  }
}));

app.get('/', function (_, res) {
  res.send('Hello harvesting-import-service');
});

app.post('/delta', async function (req, res, next) {
  try {
    const entries = new Delta(req.body).getInsertsFor('http://www.w3.org/ns/adms#status', STATUS_SCHEDULED);
    if (!entries.length) {
      console.log('Delta dit not contain potential tasks that are ready for import, awaiting the next batch!');
      return res.status(204).send();
    }
    for (let entry of entries) {
      // NOTE: we don't wait as we do not want to keep hold off the connection.
      const incrementalImportingTask = await loadExtractionTask(entry, TASK_HARVESTING_INCREMENTAL_IMPORTING);
      if (incrementalImportingTask) {
        runIncrementatlImport(incrementalImportingTask);
      }
      else {
        const importingTask = await loadExtractionTask(entry, TASK_HARVESTING_IMPORTING);
        if(importingTask) {
          runFullImport(importingTask);
        }
      }
    }
    return res.status(200).send().end();
  } catch (e) {
    console.log(`Something unexpected went wrong while handling delta harvesting-tasks!`);
    console.error(e);
    return next(e);
  }
});

app.use(errorHandler);
