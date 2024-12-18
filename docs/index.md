# Welcome to cdsobs's documentation!

The CopDS Observation Repository Catalogue Manager is intended to be used to define
and update observation datasets in the CADS.

## User guide

The CopDS Observation Repository Catalogue Manager can be used both with its
python API and with the Command Line Interface.

### Configuration

The configuration is set up in a single YAML file. An example is available in
cdsobs/data/cdsobs_config_template.yml

```yaml
# Configuration of the ingestion database where much of the input data to fill the
# repository lives on. More than one can be configured. main is the default.
ingestion_databases:
  main:
    db_user: someuser
    pwd: somepassword
    host: ingestiondb.domain.com
    port: 5432
    db_name: somename
# Configuration of the database of the observation repository catalogue.
catalogue_db:
  db_user: your_catalogue_user
  pwd: your_catalogue_pass
  host: catalogue.domain.com
  port: 5433
  db_name: cataloguedbtest
# Configuration of the S3 storage of the observation repository.
# namespace will be prepended to the bucket names, to prevent collisions.
s3config:
  access_key: some_access_key
  secret_key: some_secret_key
  host: s3.cds.ecmwf.int
  port: 443
  namespace: cds2-obs-dev
# The following are configuration variables specific for each dataset.
# Supported keywords are:
# name: dataset_name following CoPDS conventions
# lon_tile_size: size of the partitions in the longitude axis, in degrees.
# lat_tile_size: size of the partitions in the latitude axis, in degrees.
# time_tile_size: Optional, can be month and year, default is month.
# available_cdm_tables: Optional, CDM tables to be used for parsing the data.  Default is
# to use observations_table, header_table and station_configuration.
# reader: Optional, default is "cdsobs.ingestion.readers.sql.read_header_and_data_tables".
# Reader function to use to read the data into the observation repository. Can be a mapping
# to specify a different reader for each dataset_source.
# reader_extra_args: mapping to pass to the reader as extra arguments, for example
# an input dir containing data files.
datasets:
- name: insitu-observations-woudc-ozone-total-column-and-profiles
  lon_tile_size: 180
  lat_tile_size: 90
  time_tile_size: year
- name: insitu-observations-igra-baseline-network
  lon_tile_size: 45
  lat_tile_size: 45
  available_cdm_tables:
  - observations_table
  - header_table
  - station_configuration
  - sensor_configuration
  - uncertainty_table
  reader:
    IGRA: "cdsobs.ingestion.readers.sql.read_header_and_data_tables"
    IGRA_H: "cdsobs.ingestion.readers.sql.read_singletable_data"
- name: insitu-observations-gruan-reference-network
  lon_tile_size: 20
  lat_tile_size: 20
- name: insitu-observations-near-surface-temperature-us-climate-reference-network
  lon_tile_size: 20
  lat_tile_size: 20
  reader: "cdsobs.ingestion.readers.sql.read_singletable_data"
- name: insitu-comprehensive-upper-air-observation-network
  lon_tile_size: 20
  lat_tile_size: 20
  available_cdm_tables:
  - observations_table
  - header_table
  - station_configuration
  - sensor_configuration
  reader: "cdsobs.ingestion.readers.cuon.read_cuon_netcdfs"
  reader_extra_args:
    input_dir: "test"
```

### Advanced configuration of tile sizes

For big or complex datasets, longitude and latitude tile sizes can be optimized by making then
dependent on the period (year interval) and the dataset source. In the following example, we setup
global tiles for monthly data, which is lightweight, and smaller tiles for daily data. Subdaily
data is made smaller for the last decade. Keep in mind that the computer that does run the ingestion
(the make_production tool) needs to be able to at least hold one of these tiles in memory.

```yaml
- name: insitu-observations-near-surface-temperature-us-climate-reference-network
  lon_tile_size:
    USCRN_MONTHLY: 90
    USCRN_DAILY: 90
    USCRN_HOURLY: 90
    USCRN_SUBHOURLY:
      2006-2010: 30
      2010-2030: 20
  lat_tile_size:
    USCRN_MONTHLY: 90
    USCRN_DAILY: 90
    USCRN_HOURLY: 90
    USCRN_SUBHOURLY:
      2006-2010: 30
      2010-2030: 20
  available_cdm_tables:
  - observations_table
  - header_table
  - station_configuration
  - sensor_configuration
  - uncertainty_table
  reader: "cdsobs.ingestion.readers.sql.read_singletable_data"
```

### Adding data from netCDF files

Currently, the observation catalogue manager supports reading data both from a SQL database
and from a set of files. Actually it supports reading it from anywere, by writing custom
reader functions, as long as a data  table with columns consistent with the
Common Data Model is retrieved. See [Write a custom adaptor](custom_adaptor)

In order to read data in netCDF format, the "cdsobs.ingestion.readers.netcdf.read_flat_netcdfs" has to be
used. An example of a dataset using it is available in the configuration file used for the tests
(tests/data/cdsobs_config_template):

```yaml
- name: insitu-observations-woudc-netcdfs
  lon_tile_size: 180
  lat_tile_size: 90
  reader: "cdsobs.ingestion.readers.netcdf.read_flat_netcdfs"
  reader_extra_args:
    input_dir: "test"
```

This reader will parse the files in the "input_dir" path following the pattern
"{dataset}\_{source}\_YYYY_mm.nc". For example "insitu-observations-woudc-netcdfs_OzoneSonde_1969_01.nc"

These files must contain containing all variables and stations, as a simple 2D table. From a pandas.DataFrame, this can be directly achieved by chaining the to_xarray() and to_netcdf() methods.
Column names do not need to follow the Common Data Model names, but they must be mapped to them by the service_definition.json file to be provided with the data.
Data types and units should follow the Common Data Model conventions. This data format does repeat a lot of values, compared to a normalized relational model. However, note netCDF compression can be used to achieve large compression rates and save space.
Aside from netCDF, it is possible to use other formats, such as parquet. CSV text files are however discouraged because they can't properly encode metadata such as
data types, text encodings, etc. Test netCDFs can be downloaded from
[the following link](https://cloud.predictia.es/s/7QzpBEaYyiZ3o6C/download). The following is a subset
of the ncdump output of one of these files:

```commandline
netcdf insitu-observations-woudc-netcdfs_OzoneSonde_1969_01 {
dimensions:
	index = 404 ;
	string1 = 1 ;
	string4 = 4 ;
	string5 = 5 ;
	string3 = 3 ;
variables:
	int64 index(index) ;
		index:_Storage = "chunked" ;
		index:_ChunkSizes = 404 ;
		index:_DeflateLevel = 3 ;
		index:_Shuffle = "true" ;
		index:_Endianness = "little" ;
	char primary_station_id(index, string1) ;
		primary_station_id:_Storage = "chunked" ;
		primary_station_id:_ChunkSizes = 404, 1 ;
		primary_station_id:_DeflateLevel = 3 ;
		primary_station_id:_Shuffle = "true" ;
	float height_of_station_above_sea_level(index) ;
		height_of_station_above_sea_level:_FillValue = NaNf ;
		height_of_station_above_sea_level:_Storage = "chunked" ;
		height_of_station_above_sea_level:_ChunkSizes = 404 ;
		height_of_station_above_sea_level:_DeflateLevel = 3 ;
		height_of_station_above_sea_level:_Shuffle = "true" ;
		height_of_station_above_sea_level:_Endianness = "little" ;
	int64 report_id(index) ;
		report_id:_Storage = "chunked" ;
		report_id:_ChunkSizes = 404 ;
		report_id:_DeflateLevel = 3 ;
		report_id:_Shuffle = "true" ;
		report_id:_Endianness = "little" ;
	char reference_model(index, string4) ;
		reference_model:_Storage = "chunked" ;
		reference_model:_ChunkSizes = 404, 4 ;
		reference_model:_DeflateLevel = 3 ;
		reference_model:_Shuffle = "true" ;
	char sensor_model(index, string4) ;
		sensor_model:_Storage = "chunked" ;
		sensor_model:_ChunkSizes = 404, 4 ;
		sensor_model:_DeflateLevel = 3 ;
		sensor_model:_Shuffle = "true" ;
	float ozone_reference_time_mean(index) ;
		ozone_reference_time_mean:_FillValue = NaNf ;
		ozone_reference_time_mean:_Storage = "chunked" ;
		ozone_reference_time_mean:_ChunkSizes = 404 ;
		ozone_reference_time_mean:_DeflateLevel = 3 ;
		ozone_reference_time_mean:_Shuffle = "true" ;
		ozone_reference_time_mean:_Endianness = "little" ;
	int64 report_timestamp(index) ;
		report_timestamp:units = "days since 1969-01-30 05:45:00" ;
		report_timestamp:calendar = "proleptic_gregorian" ;
		report_timestamp:_Storage = "chunked" ;
		report_timestamp:_ChunkSizes = 404 ;
		report_timestamp:_DeflateLevel = 3 ;
		report_timestamp:_Shuffle = "true" ;
		report_timestamp:_Endianness = "little" ;
	char sensor_id(index, string5) ;
		sensor_id:_Storage = "chunked" ;
		sensor_id:_ChunkSizes = 404, 5 ;
		sensor_id:_DeflateLevel = 3 ;
		sensor_id:_Shuffle = "true" ;

	(...)
```

Note that the index variable is a dummy variable. Also note that char data types are used for strings
instead of the string data type available in netCDF4, which is very slow.

The service_defitinion.json required to parse these files is available in the repo in
tests/data/insitu-observations-woudc-netcdfs/service_definition.json.

### Command Line Interface

The command line interface is available under the cadsobs command:

```commandline
 Usage: cadsobs [OPTIONS] COMMAND [ARGS]...

 Copernicus Climate & Atmoshpere Data Store Observation Manager Command Line Interface

╭─ Options ───────────────────────────────────────────────────────────────────────────────────╮
│ --install-completion          Install completion for the current shell.                     │
│ --show-completion             Show completion for the current shell, to copy it or          │
│                               customize the installation.                                   │
│ --help                        Show this message and exit.                                   │
╰─────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ──────────────────────────────────────────────────────────────────────────────────╮
│ catalogue-dataset-info        Get catalogue info for certain dataset.                       │
│ check-consistency             Check if catalogue db and object storage are consistent.      │
│ copy-dataset                  Copy all catalogue datasets entries and its S3 assets.        │
│ delete-dataset                Permanently delete the given dataset from the catalogue and   │
│                               the storage.                                                  │
│ get_forms_jsons               Save the geco output json files in a folder, optionally       │
│                               upload it.                                                    │
│ list-catalogue                List entries in the catalogue. Accepts arguments to filter    │
│                               the output.                                                   │
│ list-datasets                 List all datasets and versions.                               │
│ make-cdm                      Prepare the data to be uploaded without actually uploading    │
│                               it.                                                           │
│ make-production               Upload datasets to the CADS observation repository.           │
│ retrieve                      Retrieve datasets from the CADS observation repository.       │
│ validate-service-definition   Validate a service definition YAML file.                      │
╰─────────────────────────────────────────────────────────────────────────────────────────────╯
```

#### Validate service defintion

Simply validates the correctness of the service definition JSON file. All the errors
encountered will appear on the log.

```commandline
 Usage: cadsobs validate-service-definition [OPTIONS]
                                                 SERVICE_DEFINITION_JSON

 Validate a service definition JSON file.

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    service_definition_json      TEXT  Path to JSON file [default: None] [required]                              │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                                                       │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

An example of
a valid service defintion is available at
tests/data/insitu-observations-woudc-ozone-total-column-and-profiles/service_definition.yml

See [this confluence page](https://confluence.ecmwf.int/display/CDSM/Observations+Service+Definition) for
the specification of the service definition file.

#### Make production

Runs the _ingestion_pipeline_. This pipeline passess the
input data trhough a series of steps in order to save it into the observations
catalogue and the storage components.

The **cdsobs_config** file must contain database credentials, tile size and log level. A
config template is available at tests/data/cdsobs_config_template.yml. Note that filled
values on the template are for the test instance and not valid for production.

```commandline
 Usage: cadsobs make-production [OPTIONS]

 Upload datasets to the CADS observation repository.
 Read input data for a CADS observations dataset, homogenises it, partitions it and uploads it
 to the observation catalogue and storage.

╭─ Options ───────────────────────────────────────────────────────────────────────────────────╮
│ *  --dataset      -d      TEXT     Dataset name [required]                                  │
│ *  --start-year           INTEGER  Year to start processing the data [required]             │
│ *  --end-year             INTEGER  Year to stop processing the data [required]              │
│    --config       -c      PATH     Path to the cdsobs_config yml. If not provided, the      │
│                                    function will search for the file                        │
│                                    $HOME/.cdsobs/cdsobs_config.yml                          │
│                                    [env var: CDSOBS_CONFIG]                                 │
│ *  --source               TEXT     Source to process. Sources are defined in the service    │
│                                    definition file,in the sources mapping.                  │
│                                    [default: None]                                          │
│                                    [required]                                               │
│    --update       -u               If set, data overlapping in time (year and month) with   │
│                                    existing partitions will be read in order to check if it │
│                                    changed these need to be updated. By default, these time │
│                                    intervals will be skipped.                               │
│    --start-month          INTEGER  Month to start reading the data. It only applies to the  │
│                                    first year of the interval. Default is 1.                │
│                                    [default: 1]                                             │
│    --help                          Show this message and exit.                              │
╰─────────────────────────────────────────────────────────────────────────────────────────────╯

```

#### retrieve

Retrieve CLI tool launches the _retrieve_pipeline_. It will download and filter the data in the
storage according to the parameters passed in the JSON file passed to --retrieve-params argument
(see below for an example JSON). It will save this data as a netCDF file located in the directory
defined by --output-dir argument.

```commandline
 Usage: cadsobs retrieve [OPTIONS]

 Retrieve datasets from the CADS observation repository.

╭─ Options ───────────────────────────────────────────────────────────────────────────────────╮
│    --config           -c       PATH     Path to the cdsobs_config yml. If not provided, the │
│                                         function will search for the file                   │
│                                         $HOME/.cdsobs/cdsobs_config.yml                     │
│                                         [env var: CDSOBS_CONFIG]                            │
│ *  --retrieve-params  -p       PATH     Path to a JSON file with the retrieve params.       │
│                                         [required]                                          │
│ *  --output-dir       -o       PATH     Directory where to write the output file.           │
│                                         [required]                                          │
│    --size-limit       -sl      INTEGER  Specify a size limit for the data size retrieved in │
│                                         bytes.                                              │
│    --help                               Show this message and exit.                         │
╰─────────────────────────────────────────────────────────────────────────────────────────────╯
```

The following is an example of the JSON file to be passed to --retrieve-params.

```json
[
  "insitu-observations-woudc-ozone-total-column-and-profiles",
  {
    "dataset_source": "OzoneSonde",
    "time_coverage": [
      "1961-01-01 00:00:00",
      "2022-12-31 00:00:00"
    ],
    "variables": [
      "air_temperature"
    ],
    "stations": [
      "69",
      "111",
      "76"
    ],
    "format": "netCDF"
  }
]
```

#### list-datasets

This CLI tool simply lists the datasets available in an observation repository.

```commandline
 Usage: cadsobs list-datasets [OPTIONS]

 List all datasets and versions.

╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --config        -c      PATH     Path to the cdsobs_config yml. If not provided, the function will search for the │
│                                  file $HOME/.cdsobs/cdsobs_config.yml                                             │
│                                  [env var: CDSOBS_CONFIG]                                                         │
│ --page                  INTEGER  Results are paginated by 50 entries, choose page [default: 0]                    │
│ --print-format          TEXT     Format to display results, either table or json [default: table]                 │
│ --help                           Show this message and exit.                                                      │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

#### catalogue-dataset-info

This tool shows a summary of the data loaded in the observation repository for a given
dataset.

```commandline
 Usage: cadsobs catalogue-dataset-info [OPTIONS] DATASET [SOURCE]

 Get catalogue info for certain dataset.

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    dataset      TEXT      dataset name [default: None] [required]                                               │
│      source       [SOURCE]  dataset source, if not provided all dataset sources will be displayed                 │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --config  -c      PATH  Path to the cdsobs_config yml. If not provided, the function will search for the file     │
│                         $HOME/.cdsobs/cdsobs_config.yml                                                           │
│                         [env var: CDSOBS_CONFIG]                                                                  │
│ --help                  Show this message and exit.                                                               │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

#### list-catalogue

Another tool to explore the data loaded into the repository. list-catalogue will list
all the entries of the catalogue. Accepts several arguments to filter the output by
variables, stations, etc.

```commandline
 Usage: cadsobs list-catalogue [OPTIONS]

 List entries in the catalogue. Accepts arguments to filter the output.

╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --config        -c      PATH     Path to the cdsobs_config yml. If not provided, the function will search for the │
│                                  file $HOME/.cdsobs/cdsobs_config.yml                                             │
│                                  [env var: CDSOBS_CONFIG]                                                         │
│ --page                  INTEGER  Results are paginated by 50 entries, choose a page number [default: 1]           │
│ --dataset               TEXT     filter by dataset name                                                           │
│ --source                TEXT     filter by source name                                                            │
│ --time                  TEXT     Filter by an exact date or by an interval of two dates. For example: to retrieve │
│                                  all partitions of year 1970: 1970-1-1,1970-12-31                                 │
│ --latitudes             TEXT     Filter by an exact latitude or by an interval of two latitudes                   │
│ --longitudes            TEXT     Filter by an exact longitude or by an interval of two longitudes                 │
│ --variables             TEXT     Filter by a variable or a list of variables. For example:to retrieve all         │
│                                  partitions that contain variables air_pressure and/or air_temperature:           │
│                                  air_pressure,air_temperature                                                     │
│ --stations              TEXT     Filter by a station or a list of stations                                        │
│ --print-format          TEXT     Format to display results, either table or json [default: table]                 │
│ --help                           Show this message and exit.                                                      │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

#### check-consistency

This tool runs a sanity check over the repository. It checks every asset in the storage
has a  catalogue entry and vice versa.

```commandline
 Usage: cadsobs check-consistency [OPTIONS] DATASET

 Check if catalogue db and object storage are consistent.
 That means that every asset has a catalogue entry and vice versa.

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    dataset      TEXT  dataset name. If provided will only check entries for that dataset. [default: None]       │
│                         [required]                                                                                │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --config  -c      PATH  Path to the cdsobs_config yml. If not provided, the function will search for the file     │
│                         $HOME/.cdsobs/cdsobs_config.yml                                                           │
│                         [env var: CDSOBS_CONFIG]                                                                  │
│ --help                  Show this message and exit.                                                               │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

```

#### copy-dataset

The copy dataset command is intended to be used to move a dataset from a test environment to a
production environment.

```commandline
 Usage: cadsobs copy-dataset [OPTIONS]

 Copy all catalogue datasets entries and its S3 assets.
 Choose to copy inside the original databases with a new name (dest_dataset) or provide a different
 Catalogue DB and S3 credentials to insert copies.
 Parameters ---------- cdsobs_config_yml dataset dest_config_yml dest_dataset -------

╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────╮
│    --config           -c      PATH  Path to the cdsobs_config yml. If not provided, the function     │
│                                     will search for the file $HOME/.cdsobs/cdsobs_config.yml         │
│                                     [env var: CDSOBS_CONFIG]                                         │
│ *  --dataset                  TEXT  dataset to copy [default: None] [required]                       │
│    --dest-config-yml          PATH  Path to the cdsobs_config.yml with destination database          │
│                                     credentials. Must contain both s3 and catalogue db credentials.  │
│                                     If not provided, the dataset will be copied inside the initial   │
│                                     databases                                                        │
│                                     [default: None]                                                  │
│    --dest-dataset             TEXT  Destination name for the dataset. If not provided, the original  │
│                                     name will be used.                                               │
│                                     [default: None]                                                  │
│    --help                           Show this message and exit.                                      │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

#### delete-dataset

This command is to be used to completely remove a dataset. Sometimes updating it will not be
possible and a fresh "make_production" command will have to be run from zero. In this case, note
that the dataset version will be reset to 1.0 in the catalogue.

```commandline
 Usage: cadsobs delete-dataset [OPTIONS]

 Permanently delete the given dataset from the catalogue and the storage.

╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────╮
│    --config   -c      PATH  Path to the cdsobs_config yml. If not provided, the function will search │
│                             for the file $HOME/.cdsobs/cdsobs_config.yml                             │
│                             [env var: CDSOBS_CONFIG]                                                 │
│ *  --dataset          TEXT  dataset to delete [default: None] [required]                             │
│    --help                   Show this message and exit.                                              │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────╯

```

#### get-forms-jsons

```commandline
 Usage: cadsobs get_forms_jsons [OPTIONS]

 Save the geco output json files in a folder, optionally upload it.

╭─ Options ───────────────────────────────────────────────────────────────────────────────────╮
│ *  --dataset        -d      TEXT  Dataset name [required]                                   │
│    --config         -c      PATH  Path to the cdsobs_config yml. If not provided, the       │
│                                   function will search for the file                         │
│                                   $HOME/.cdsobs/cdsobs_config.yml                           │
│                                   [env var: CDSOBS_CONFIG]                                  │
│    --output-dir     -o      PATH  Directory where to write the output if --save-data is     │
│                                   enabled.                                                  │
│                                   [default: /tmp]                                           │
│    --upload         -u                                                                      │
│    --stations_file  -s                                                                      │
│    --help                         Show this message and exit.                               │
╰─────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Python API

It is recommended to use the Command Line Interface, as is it simpler to use than the
python API. However, here we document the main entrypoints.

The main function is {py:func}`cdsobs.api.run_ingestion_pipeline` which runs the
ingestion pipeline that populates the observations repository with data.

The second key function is {py:func}`cdsobs.retrieve.api.retrieve_observations`
which is the way of retrieving data from the repository in netCDF format.

```{toctree}
:caption: 'Contents:'
:maxdepth: 2

API Reference <_api/cdsobs/index>
```

# Indices and tables

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`
