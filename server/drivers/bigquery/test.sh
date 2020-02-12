#!/bin/bash

if [ ! -z ${var+BIGQUERY_TEST_CREDENTIALS_FILE} ]; then
  echo "Must set BIGQUERY_TEST_CREDENTIALS_FILE with service account json file for testing"
  exit 1
fi

if [ ! -z ${var+BIGQUERY_TEST_GCP_PROJECT_ID} ]; then
  echo "Must set BIGQUERY_TEST_GCP_PROJECT_ID with Google Cloud Project Name for testing"
  exit 2
fi

if [ ! -z ${var+BIGQUERY_TEST_DATASET_NAME} ]; then
  echo "Must set BIGQUERY_TEST_DATASET_NAME with BigQuery Dataset Name"
  exit 3
fi

if [ ! -z ${var+BIGQUERY_TEST_DATASET_LOCATION} ]; then
  echo "Must set BIGQUERY_TEST_DATASET_LOCATION with Google Cloud Project Region e.g. asia-southeast1"
  exit 4
fi

npx mocha ./test.js
