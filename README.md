# eia-860-data-summary
A series of scripts that download, transform, and model eia 860 power plant data

Series of scripts does the following:
1) Extracts eia 860 files from the website. The script then does some transformation on the files, concats them, and drops them in storage.
- eia-download-light-transform.py [In progress: Refactor to include functions]
2) Queries data from postgres and loads them into a staging area. [In progress]
3) Transforms staging files using dimensional modeling.[In progress]
4) Then puts the dim and fact tables in a mart.[In progress]

*NOTE:
  - dbt with postgres only supports 1)the target schema and source schema being in the same database and 2) 1 target schema per project
  - therefore, the dbt portion of this repository is split into 2 dbt projects in the "dbt-models" directory. 
