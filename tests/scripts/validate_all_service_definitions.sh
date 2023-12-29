#!/bin/bash
export CADSOBS_LOGGING_FORMAT="CONSOLE"
export CADSOBS_LOGGING_LEVEL="INFO"
export CLI_DEBUG=true

for file in ../../cdsobs/data/*/service_definition.yml
do
  echo "Validation ${file}..."
  cadsobs validate-service-definition-json ${file}
done
