#!/bin/bash
set -e
# Load secrets
source ../../.env
# Output file
PROJECT_ROOT="$(realpath ../../)"
TEST_DOCKER_DIR="${PROJECT_ROOT}/tests/docker"
OUTPUT_FILE="${TEST_DOCKER_DIR}/docker-entrypoint-initdb.d/test_ingestiondb.sql"
TABLES_FILE="${PROJECT_ROOT}/tests/scripts/tables_to_copy.txt"
IFS=$'\n'
# Number of lines
LIMIT="500"
#
# Functions
#
function dump_ingestion_table_schema(){
  table_name=$1
  is_header=$2
  db_name="$3"
  if [ "$is_header" = "True" ]
  then
    ofile="/tmp/${table_name}_schema_header.sql"
  else
    ofile="/tmp/${table_name}_schema_data.sql"
  fi

  pg_dump  -U ${INGESTION_DB_USER} -h localhost -p ${INGESTION_DB_LOCAL_PORT} \
    --schema-only -t "${table_name}" ${db_name} > "${ofile}"
}

function dump_data_table_with_limit(){
  table_name=$1
  db_name=$2

  if [[ ${data_table} == "igra_h" ]]
  then
    SORT_KEY="actual_time"
  else
    SORT_KEY="id"
  fi

  psql  -U ${INGESTION_DB_USER} -h localhost -p ${INGESTION_DB_LOCAL_PORT} -d ${db_name} \
    -c 'COPY (SELECT * FROM '${table_name}' ORDER BY '${SORT_KEY}' LIMIT '${LIMIT}') TO STDOUT;' > /tmp/${table_name}_data.tsv
}


function dump_ingestion_tables_data_with_limit(){
  header_table=$1
  data_table=$2
  header_index=$3
  header_index_in_data=$4
  db_name=$5
  # In some cases we want to query from a yearly partition because otherwise the
  # the order by takes too long.
  if [[ ${data_table} == "guan_data_value" ]]
  then
    data_table_to_use="guan_data_value_2005"
  else
    data_table_to_use="${data_table}"
  fi

  if [[ ${data_table} == "igra_h" ]]
  then
    SORT_KEY="actual_time"
  else
    SORT_KEY="id"
  fi

  # Dump 500 lines of data
  psql  -U ${INGESTION_DB_USER} -h localhost -p ${INGESTION_DB_LOCAL_PORT} -d ${db_name} \
    -c 'COPY (SELECT * FROM '${data_table_to_use}' ORDER BY '${SORT_KEY}' LIMIT '${LIMIT}') TO STDOUT;' > /tmp/${data_table}_data.tsv
  # Dump the related headers with a subquery
  psql  -U ${INGESTION_DB_USER} -h localhost -p ${INGESTION_DB_LOCAL_PORT} -d ${db_name} \
    -c 'COPY (SELECT * FROM '${header_table}' WHERE '${header_index}'
    in (SELECT '${header_index_in_data}' FROM '${data_table_to_use}' ORDER BY '${SORT_KEY}' LIMIT '${LIMIT}')) TO STDOUT;' > /tmp/${header_table}_data.tsv
}

function fill_test_table_from_file(){
  table_name=$1
  db_name=$2
  psql  -d ${INGESTION_DB_NAME} -U ${TEST_INGESTION_DB_USER} -h ${TEST_INGESTION_DB_HOST} -d ${db_name} \
    -p ${TEST_INGESTION_DB_PORT} -w -c "COPY ${table_name} from STDIN;" < /tmp/${table_name}_data.tsv
}
#
# Program
#
# Create SSH tunnel
ssh -f -N -L ${INGESTION_DB_LOCAL_PORT}:${INGESTION_DB_HOST}:${INGESTION_DB_PORT} ${VM_HOST}
# Dump the roles definition
pg_dumpall -r  --no-role-passwords -U ${INGESTION_DB_USER} -h localhost \
  -p ${INGESTION_DB_LOCAL_PORT} > /tmp/roles.sql
# Dumps the functions and extensions definitions
pg_dump  -U ${INGESTION_DB_USER} -h localhost -p ${INGESTION_DB_LOCAL_PORT} \
  --verbose --exclude-table='public.*' --exclude-table='uscrn.*' --exclude-schema='uscrn' -d ${INGESTION_DB_NAME} > /tmp/functions.sql
# Dump the schema and data of the tables
rm -f /tmp/*_schema.sql
for row in $(cat "${TABLES_FILE}" | grep -v "#")
do
  OLDIFS=$IFS
  IFS="," read header_table data_table header_index  header_index_in_data db_name <<< "${row}"
  IFS="$OLDIFS"
  if [[ ${header_table} != "None" ]]
  then
    dump_ingestion_table_schema "${header_table}" "True" "${db_name}"
  fi
  dump_ingestion_table_schema "${data_table}" "False" "${db_name}"
  if [[ ${header_table} != "None" ]]
  then
    dump_ingestion_tables_data_with_limit "${header_table}" "${data_table}" "${header_index}" "${header_index_in_data}" "${db_name}"
  else
    dump_data_table_with_limit "${data_table}" "${db_name}"
  fi
done
# Kill the SSH tunnel
kill "$(ps -ef | grep ssh | grep "${INGESTION_DB_HOST}" | awk 'NR==1 {print $2}')"
# Go to the docker folder
cd "${TEST_DOCKER_DIR}"
# Merge the dumped sql files and clean them
rm -f "${OUTPUT_FILE}"
# It is important that headers go first
# Some things (triggers, partitions) are removed here because they give problems.
cat /tmp/roles.sql /tmp/functions.sql /tmp/*_schema_header.sql  /tmp/*_schema_data.sql | \
  grep -v "CREATE TRIGGER" |  sed s/'PARTITION BY RANGE (date_of_observation)'// | \
  sed s/'PARTITION BY RANGE (timestamp_datetime)'// | \
  sed s/'PARTITION BY RANGE (timestamp_datetime_first_day)'// | \
  sed s/'PARTITION BY RANGE (daily_date)'// | \
  sed s/'PARTITION BY RANGE (report_timestamp)'// > \
  "${OUTPUT_FILE}"
rm /tmp/*.sql
# Deploy the testing database, this will automatically run the code in ./test_ingestiondb.sql
docker compose down
docker compose up -d
sleep 20
# COPY the data dumped from the tables
# We need to use a password here as the postgres docker is configured to require one
export PGPASSWORD="${TEST_INGESTION_DB_PASS}"
for row in $(cat "${TABLES_FILE}" | grep -v "#")
do
  OLDIFS=$IFS
  IFS="," read header_table data_table header_index  header_index_in_data db_name <<< "${row}"
  IFS="$OLDIFS"
  if [[ ${header_table} != "None" ]]
  then
    fill_test_table_from_file "${header_table}" "baron"
  fi
  fill_test_table_from_file "${data_table}" "baron"
done
# Dump the filled test database and override the main sql file
# We need to skip the line creating the DATABASE and the ROLE user, as these are
# automatically created by the docker and will raise an error
pg_dumpall -U "${TEST_INGESTION_DB_USER}" -h "${TEST_INGESTION_DB_HOST}" \
  -p "${TEST_INGESTION_DB_PORT}"  | grep -v 'ROLE '${TEST_INGESTION_DB_USER}'' | \
  grep -v 'CREATE DATABASE' > ${OUTPUT_FILE}
# Clean the container we used
cd "${TEST_DOCKER_DIR}"
docker compose down
