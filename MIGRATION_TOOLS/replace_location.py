'''
This script is going to read DDL statements extrated from some metastore 
and replace locations with a different object storage path
'''

source_path = "/Users/antonio.pinheirofilho/Documents/Tools/code-testing/metastore"
match_string = "LOCATION 'dbfs:/"
replace_string = "LOCATION 'newLoc:/"

import os

for path, subdirs, files in os.walk(source_path):
    for name in files:
        if not name.startswith("."):
            file_path = os.path.join(path, name)

            # Read in the file
            with open(file_path, "r") as file:
                filedata = file.read()

                # Replace the target string
                filedata = filedata.replace(match_string, replace_string)

                # Overwrite content
                with open(file_path, 'w') as file:
                    file.write(filedata)