const assert = require('assert');
const bigquery = require('./index.js');

const connection = {
  name: 'test bigquery',
  driver: 'bigquery',
  projectId: process.env.BIGQUERY_TEST_GCP_PROJECT_ID,
  datasetName: process.env.BIGQUERY_TEST_DATASET_NAME,
  keyFile: process.env.BIGQUERY_TEST_CREDENTIALS_FILE,
  datasetLocation: process.env.BIGQUERY_TEST_DATASET_LOCATION,
  maxRows: 10
};

// const checkTableSize = `SELECT size_bytes FROM \`${connection.datasetName}.__TABLES__\` WHERE table_id='test'`;
const testTable = 'sqlpad_test';
const dropTable = `DROP TABLE IF EXISTS ${connection.datasetName}.${testTable}`;
const createTable = `CREATE TABLE ${connection.datasetName}.${testTable} (id int64)`;
const inserts = `INSERT INTO ${connection.datasetName}.${testTable} (id) VALUES (1), (2), (3)`;
const testTimeoutMsecs = 10000;

describe('drivers/bigquery', function() {
  this.timeout(testTimeoutMsecs); // Set a large default timeout for all tests because BigQuery can be slow to respond.

  before(function() {
    return bigquery
      .runQuery(dropTable, connection)
      .then(() => bigquery.runQuery(createTable, connection))
      .then(() => bigquery.runQuery(inserts, connection));
  });

  it('implements testConnection', function() {
    return bigquery.testConnection(connection).then(results => {
      assert(results);
      assert(!results.incomplete);
    });
  });

  it('implements getSchema', function() {
    return bigquery.getSchema(connection).then(schemaInfo => {
      const schema = schemaInfo[connection.datasetName];
      assert(schema);
      assert(schema.hasOwnProperty(testTable));
      const columns = schema[testTable];
      assert.equal(columns.length, 1);
      assert.equal(columns[0].table_schema, connection.datasetName);
      assert.equal(columns[0].table_name, testTable);
      assert.equal(columns[0].column_name, 'id');
      assert.equal(columns[0].data_type, 'INTEGER');
    });
  });

  it('implements runQuery and runs queries under a limit', function() {
    return bigquery
      .runQuery(
        `SELECT id FROM ${connection.datasetName}.${testTable} WHERE id = 1`,
        connection
      )
      .then(results => {
        assert(!results.incomplete);
        assert.equal(results.rows.length, 1);
      });
  });

  it('implements runQuery and runs queries capped at a limit', function() {
    const limitedConnection = { ...connection, maxRows: 2 };

    return bigquery
      .runQuery(
        `SELECT * FROM ${connection.datasetName}.${testTable}`,
        limitedConnection
      )
      .then(results => {
        console.log(`results: ${JSON.stringify(results, null, 2)}`);
        assert(results.incomplete);
        assert.equal(results.rows.length, 2);
      });
  });

  it('returns descriptive error message for a missing table', function() {
    let error;

    return bigquery
      .runQuery(
        `SELECT * FROM ${connection.datasetName}.missing_table`,
        connection
      )
      .catch(e => {
        error = e;
      })
      .then(() => {
        assert(error);
        assert(error.toString().includes('was not found'));
      });
  });
});
