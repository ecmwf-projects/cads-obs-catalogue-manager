#!/bin/bash
# WOUDC
dataset="insitu-observations-woudc-ozone-total-column-and-profiles"

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 1962 --end-year 2022 \
  --source OzoneSonde  >& make_production_woudc_ozonesonde.log

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 1924 --end-year 2022 \
  --source TotalOzone  >& make_production_woudc_totalozone.log

# IGRA

dataset="insitu-observations-igra-baseline-network"

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 1978 --end-year 2020 \
  --source IGRA  >& make_production_igra_igra.log

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 1978 --end-year 2020 \
  --source IGRA_H  >& make_production_woudc_igra_igra-h.log

# GRUAN

dataset="insitu-observations-gruan-reference-network"
cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 2006 --end-year 2020   \
  --source GRUAN  >& make_production_gruan.log

# GNSS

dataset="insitu-observations-gnss"

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 1996 --end-year 2014   \
  --source EPN  >& make_production_gnss_epn.log

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 1996 --end-year 2014   \
  --source IGS  >& make_production_gnss_igs.log

# USCRN

dataset="insitu-observations-near-surface-temperature-us-climate-reference-network"

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 2006 --end-year 2022   \
  --source USCRN_DAILY  >& make_production_urscrn_daily.log

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 2006 --end-year 2022   \
  --source USCRN_HOURLY  >& make_production_urscrn_hourly.log

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 2006 --end-year 2022   \
  --source USCRN_SUBHOURLY  >& make_production_urscrn_subhourly.log

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 2006 --end-year 2022   \
  --source USCRN_MONTHLY  >& make_production_urscrn_monthly.log

# CUON
dataset="insitu-comprehensive-upper-air-observation-network"

cadsobs make-production -d ${dataset} \
  -s cads-obs-catalogue-manager/cdsobs/data/${dataset}/service_definition.yml \
  -c cdsobs_config.yml --start-year 1924 --end-year 2023 >& make_production_cuon.log
