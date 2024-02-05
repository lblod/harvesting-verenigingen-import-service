import {app, errorHandler} from 'mu';

import bodyParser from 'body-parser';
import { run as runImportPipeline } from './lib/pipeline-import';
import { Delta } from "./lib/delta";
import {
  failBusyImportTasks,
} from "./lib/task";
import { STATUS_SCHEDULED } from './constants';


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
      runImportPipeline(entry);
    }
    return res.status(200).send().end();
  } catch (e) {
    console.log(`Something unexpected went wrong while handling delta harvesting-tasks!`);
    console.error(e);
    return next(e);
  }
});

app.use(errorHandler);
