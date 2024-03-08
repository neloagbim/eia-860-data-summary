# eia-860-data-summary
A series of scripts that download, transform, and model eia 860 power plant data

Series of scripts does the following:
1) Extracts eia 860 files from the website. The script then does some transformation on the files, concats them, and drops them in storage.
- [eia-download-light-transform.py](https://github.com/neloagbim/eia-860-data-summary/blob/main/eia-download-light-transform.py) 
  - [In progress: REFACTOR TO USE FUNCTIONAL PROGRAMMING PARADIGM]
2) Uses dbt to query and transform raw data from postgres and loads them into a staging area. [Complete]
- Union: https://github.com/neloagbim/eia-860-data-summary/blob/main/dbt-models/eia860_prj/models/staging/stg_reports_combined.sql
- Additional cleaning: https://github.com/neloagbim/eia-860-data-summary/blob/main/dbt-models/eia860_prj/models/staging/stg_clean.sql
3) Transforms staging files using dimensional modeling. [Complete]
4) Then puts the dim and fact tables in a mart. [Complete]

Lineage Graph as of March 2024:
![image](https://github.com/neloagbim/eia-860-data-summary/assets/47784696/c22eff74-15fe-4822-b85f-1c0e6bbc4aeb)

*NOTE:
  - dbt with postgres only supports the target schema and source schema being in the same database.
  - therefore, the dbt portion of this repository is all in 1 database with 3 different schemas: raw, staging, and datawarehouse
