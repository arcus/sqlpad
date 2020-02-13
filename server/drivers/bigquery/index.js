const { BigQuery } = require('@google-cloud/bigquery');
const { formatSchemaQueryResults } = require('../utils');

const id = 'bigquery';
const name = 'BigQuery';

/**
 * Run query for connection
 * Should return { rows, incomplete }
 * @param {string} query
 * @param {object} connection
 */
function runQuery(query, connection) {
  const bigquery = new BigQuery({
    projectId: connection.projectId || process.env.GCP_PROJECT,
    keyFilename: connection.keyFile || process.env.GOOGLE_CREDENTIALS_FILE
  });

  let appendedLimit = false;

  if (query.match(/^\\s*SELECT.*/)) {
    // If the query has no LIMIT, inject our own LIMIT of maxrows + 1.
    // The actual # of rows returned in this case will be maxrows, but with the `incomplete` flag set.
    // TODO: I don't know if tacking a LIMIT on at the end will do what we want in all cases. (E.g. UNION?)
    const match = query.match(/.*LIMIT +(\\d+);?\\s*$/);
    if (!match) {
      appendedLimit = true;
      query += ` LIMIT ${connection.maxRows + 1}`;
    } else {
      // TODO: does the user-specific LIMIT override connection.maxrows? (I'm assuming yes for now).
      const limit = parseInt(match[1], 10);
      if (limit > connection.maxRows) {
        console.log(
          `Detected LIMIT ${limit} greater than connection.maxRows ${connection.maxRows}; honoring the LIMIT`
        );
      }
    }
  }

  const incomplete = false;

  const options = {
    query,
    // Location must match that of the dataset(s) referenced in the query.
    location: connection.datasetLocation
  };

  return bigquery
    .createQueryJob(options)
    .then(([job]) => {
      // Waits for the query to finish
      return job.getQueryResults({
        maxResults: connection.maxRows + 1
      });
    })
    .then(([rows]) => {
      return {
        incomplete,
        rows
      };
    });
}
// TODO: IMPLEMENT ME

//   return new Promise((resolve, reject) => {
//     let incomplete = false
//     const rows = []
//
//     const options = {
//       query: query,
//       location: connection.location,
//     }
//
//     const myQueryJob = myConnection.createQueryJob(options)
//     myQueryJob.getQueryResults()
//       .then((rows) => {
//         if (rows.length > connection.maxRows) {
//           rows.splice(connection.maxRows)
//           incomplete = true
//         }
//         continueOn()
//       })
//
//     myQuery.then((rows) => {
//       // If we haven't hit the max yet add row to results
//
//       // Too many rows
//       incomplete = true
//
//       // Destroy the underlying connection
//       // Calling end() will wait and eventually time out
//       myConnection.destroy()
//       continueOn()
//     })
//
//     myQuery
//       .on('error', function(err) {
//         // Handle error,
//         // an 'end' event will be emitted after this as well
//         // so we'll call the callback there.
//         queryError = err
//       })
//       .on('result', function(row) {
//
//       })
//       .on('end', function() {
//         // all rows have been received
//         // This will not fire if we end the connection early
//         // myConnection.end()
//         // myConnection.destroy()
//         myConnection.end(error => {
//           if (error) {
//             console.error('Error ending MySQL connection', error)
//           }
//           continueOn()
//         })
//       })
//     })
//   })
// }

/**
 * Test connectivity of connection
 * @param {*} connection
 */
function testConnection(connection) {
  const query = `SELECT * FROM \`${connection.datasetName}.__TABLES_SUMMARY__\` LIMIT 1`;
  return runQuery(query, connection);
}

/**
 * Get schema for connection
 * @param {*} connection
 */
function getSchema(connection) {
  const bigquery = new BigQuery({
    projectId: connection.projectId || process.env.GCP_PROJECT,
    keyFilename: connection.keyFile || process.env.GOOGLE_CREDENTIALS_FILE
  });

  const options = {
    query: `SELECT * FROM ${connection.datasetName}.__TABLES__`,
    // Location must match that of the dataset(s) referenced in the query.
    location: connection.datasetLocation
  };

  return bigquery
    .createQueryJob(options)
    .then(([job]) => {
      // Waits for the query to finish
      return job.getQueryResults();
    })
    .then(([tables]) => {
      const promises = [];
      for (let i in tables) {
        const table = tables[i];
        promises.push(
          bigquery
            .dataset(connection.datasetName)
            .table(table.table_id)
            .getMetadata()
        );
      }
      return Promise.all(promises);
    })
    .then(tables => {
      const tableSchema = {
        rows: []
      };
      for (let i in tables) {
        const tableInfo = tables[i][0];
        if (tableInfo.kind !== 'bigquery#table') continue;
        const datasetId = tableInfo.tableReference.datasetId;
        const tableId = tableInfo.tableReference.tableId;
        const fields = tableInfo.schema.fields;
        for (let i in fields) {
          const field = fields[i];
          tableSchema.rows.push({
            table_schema: datasetId,
            table_name: tableId,
            column_name: field.name,
            data_type: field.type
          });
        }
      }
      return formatSchemaQueryResults(tableSchema);
    });
}

const fields = [
  {
    key: 'projectId',
    formType: 'TEXT',
    label: 'Google Cloud Console Project ID'
  },
  {
    key: 'keyFile',
    formType: 'TEXT',
    label: 'JSON Keyfile for Service Account'
  },
  {
    key: 'datasetName',
    formType: 'TEXT',
    label: 'Dataset to use'
  },
  {
    key: 'datasetLocation',
    formType: 'TEXT',
    label: 'Location for this Dataset'
  }
];

module.exports = {
  id,
  name,
  fields,
  getSchema,
  runQuery,
  testConnection
};
