# Description

This script will load 1 year of all datasets considered into a test instance of the CADS observation repository,
and will retrieve some data. The execution times measured will be saved in a CSV file.

# Usage

Copy cdsobs.yml to this directory.

Be sure that the namespaces and the catalogue database name are not pointing to anything operational or
used by others. Create another database with "CREATE DATABASE cdsobs-bench;" in the SQL terminal if needed.

Run

```commandline
python 1_year_benchmark.py
```

The resulting times measured will be written to results_1_year_benchmark.csv  CSV file.
