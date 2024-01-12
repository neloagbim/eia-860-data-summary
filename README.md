# eia-860-data-summary
A series of scripts that download, transform, and model eia 860 power plant data

Series of scripts does the following:
1) Extracts eia 860 files from the website. The script then does some transformation on the files, concats them, and drops them in storage.
2) Calls files from azure and loads them into a staging area.
3) Transforms staging files using dimensional modeling.
4) Then puts the dim and fact tables in a mart.
