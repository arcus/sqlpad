const assert = require('assert')
const bigquery = require('./index.js')

const connection = {
  name: 'test bigquery',
  driver: 'bigquery',
  projectId: 'publicdata', //process.env.BIGQUERY_TEST_GCP_PROJECT_ID,
  datasetName: 'bigquery-public-data.new_york',
  keyFile: process.env.BIGQUERY_TEST_CREDENTIALS_FILE,
  datasetLocation: 'US',
  maxRows: 10
}

const checkTableSize = `SELECT size_bytes FROM \`${
  connection.datasetName
}.__TABLES__\` WHERE table_id='gscd'`
// const dropTable = 'DROP TABLE IF EXISTS test;'
// const createTable = 'CREATE TABLE test (id int);'
// const inserts = 'INSERT INTO test (id) VALUES (1), (2), (3);'

describe('drivers/bigquery', function() {
  // before(function() {
  //   this.timeout(10000)
  //   return bigquery
  //     .runQuery(dropTable, connection)
  //     .then(() => bigquery.runQuery(createTable, connection))
  //     .then(() => bigquery.runQuery(inserts, connection))
  // })

  it('tests connection', function() {
    return bigquery.testConnection(connection)
  })

  it('getSchema()', function() {
    return bigquery.getSchema(connection).then(schemaInfo => {
      console.log(schemaInfo)
      assert(schemaInfo.sqlpad, 'sqlpad')
      assert(schemaInfo.sqlpad.test, 'sqlpad.test')
      const columns = schemaInfo.sqlpad.test
      assert.equal(columns.length, 1, 'columns.length')
      assert.equal(columns[0].table_schema, 'sqlpad', 'table_schema')
      assert.equal(columns[0].table_name, 'test', 'table_name')
      assert.equal(columns[0].column_name, 'id', 'column_name')
      assert(columns[0].hasOwnProperty('data_type'), 'data_type')
    })
  })

  it('runQuery under limit', function() {
    return bigquery
      .runQuery('SELECT id FROM test WHERE id = 1;', connection)
      .then(results => {
        assert(!results.incomplete, 'not incomplete')
        assert.equal(results.rows.length, 1, 'rows length')
      })
  })

  it('runQuery over limit', function() {
    const limitedConnection = Object.assign({}, connection, { maxRows: 2 })
    return bigquery
      .runQuery('SELECT * FROM test;', limitedConnection)
      .then(results => {
        assert(results.incomplete, 'incomplete')
        assert.equal(results.rows.length, 2, 'row length')
      })
  })

  it('returns descriptive error message', function() {
    let error
    return bigquery
      .runQuery('SELECT * FROM missing_table;', connection)
      .catch(e => {
        error = e
      })
      .then(() => {
        assert(error)
        assert(error.toString().indexOf('missing_table') > -1)
      })
  })
})
