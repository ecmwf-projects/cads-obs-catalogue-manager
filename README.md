# CopDS Observations repository catalogue manager

## Instalation

The CLI services will be used directly on the VM.

### Installation with conda

To install the package, simply clone it and install its dependencies via conda.

```commandline
git clone git@github.com:ecmwf-projects/cads-obs-catalogue-manager.git
cd cads-obs-catalogue-manager
conda create -n cads-obs -c conda-forge python=3.10
conda activate cads-obs
conda env update --file environment.yml
pip install --no-deps .
```

### Installation with pip

Install python 3.12 with your package manager, together with [the netCDF library](https://github.com/Unidata/netcdf-c). Other dependencies may be needed depending
on the Linux version to be used. Then run.

```commandline
git clone git@github.com:ecmwf-projects/cads-obs-catalogue-manager.git
cd cads-obs-catalogue-manager
python -m pip install .
```

## Workflow for developers/contributors

For best experience create a new conda environment (e.g. DEVELOP) with Python 3.10:

```commandline
conda create -n DEVELOP -c conda-forge python=3.12
conda activate DEVELOP
```

Before pushing to GitHub, run the following commands:

1. Update conda environment: `make conda-env-update`
1. Install this package: `pip install -e .`
1. Sync with the latest [template](https://github.com/ecmwf-projects/cookiecutter-conda-package) (optional): `make template-update`
1. Run quality assurance checks: `make qa`
1. Run tests: `make unit-tests`
1. Run the static type checker: `make type-check`
1. Build the documentation (see [Sphinx tutorial](https://www.sphinx-doc.org/en/master/tutorial/)): `make docs-build`

### Writing tests

We use the pytest framework and fixtures.

### Test data preparison

The tests are meant to work against test databases, so a few steps are needed to
set them up:

First, download the dump: [test_ingestiondb.sql](https://cloud.predictia.es/s/R9a6z8fBZQcPrAQ)
file and store it at tests/docker.

Download also [cuon_data.tar.gz](https://cloud.predictia.es/s/dTb87RQXfgJ6S6S) and
extract it in tests/data/cuon_data. This contains a couple of netCDFs from the CUON
soundings dataset for the tests.

Add a .env file at tests/docker containing the pass credentials for the 3 db:

```commandline
TEST_INGESTION_DB_PASS=xxxx
CATALOGUE_PASSWORD=xxxx
STORAGE_PASSWORD=xxxx
```

Note that credentials must coincide with the ones at the cdsobs_config template.

Finally start the docker containers by running:

```commandline
cd tests/docker
docker compose up -d
```

Ingestion db tables available right now:

- woudc_ozonesonde_header
- woudc_ozonesonde_data
- woudc_totalozone_header
- woudc_totalozone_data
- guan_data_header
- guan_data_value
- header
- harmonized_data
- gruan_data_header
- gruan_data_value
- uscrn.unc_subhourly
- uscrn.unc_hourly
- uscrn.unc_daily
- uscrn.unc_monthly

### Test data update

This requires SSH access to the VM and a .env file defining the DB connection
details. It will replicate the ingestion DB (right now only the Ozone dataset),
truncating the data tables at 10k rows, and dump it to a .sql file.

```commandline
cd tests/scripts
bash make_test_ingestiondb.sh
```

### HTTP API

To deploy the HTTP API, unicorn needs to be installed. Please note that it should
not be exposed to the internet without a proxy. The deploy command is:

```
python cdsobs/api_rest/main.py
```

The /doc endpoint can be opened in a browser to check the automatically generated
documentation.
