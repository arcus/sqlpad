const { BigQuery } = require('@google-cloud/bigquery');
const { formatSchemaQueryResults } = require('../utils');

const id = 'bigquery';
const name = 'BigQuery';

/**
 * Run query for connection
 * Should return { rows, incomplete }
 * @param {string} queryString
 * @param {object} connection
 */
function runQuery(queryString, connection) {
  const bigquery = new BigQuery({
    projectId: connection.projectId || process.env.GCP_PROJECT,
    keyFilename: connection.keyFile || process.env.GOOGLE_CREDENTIALS_FILE
  });

  let incomplete = false;

  const query = {
    query: queryString,
    // Location must match that of the dataset(s) referenced in the query.
    location: connection.datasetLocation
  };

  // TODO: should maxRows apply to non-SELECT statements?
  return bigquery
    .createQueryJob(query)
    .then(([job]) => {
      // Waits for the query to finish
      return job.getQueryResults({
        maxResults: connection.maxRows + 1
      });
    })
    .then(([rows]) => {
      if (rows.length > connection.maxRows) {
        rows.splice(connection.maxRows);
        incomplete = true;
      }
      return {
        incomplete,
        rows
      };
    });
}

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
