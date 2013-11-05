#Columby worker Supervisor process

## Description


## Process overview
run.php calls for check_queue every minute
  check_queue() checks if there are numbers of items to be processed and returns true or false
  	Render() processes the item. 



## Table description
ID					: primary key for this table
UUID				: uuid for this dataset (drupal-node and tablename)
type				: type of upload (currently csv and arcgis)
uid 				: id of the user who uploaded the file
processing			: 1 if currently in progress
processed			: % of total to be processed
done				: 1 when done, default 0
error				: Error message
total				: Total number of rows
data 				: 
url 				: url for service to be harvested
datafile_uri 		: location of data-file
metadatafile_uri 	: location of metadata-file
split 				: ??
stats 				: statistics of service
createdAt			: timestamp of creation
updatedAt			: timestamp of last update
