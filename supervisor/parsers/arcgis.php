<?php

function hr_memory_usage() {
  $mem_usage = memory_get_usage(true);

  if ($mem_usage < 1024)
    return $mem_usage." bytes";
  elseif ($mem_usage < 1048576)
    return round($mem_usage/1024,2)." kilobytes";
  else
    return round($mem_usage/1048576,2)." megabytes"; 
}


class scraper {

  public $url = "http://www.denhaag.nl/ArcGIS/rest/services/DsbBeheer/Bomenbeheer/MapServer/4";

  // Number of items to query per request
  public $cycle = 100;
  
  private $uuid; 
  private $tablename; 
  private $stats; 
  private $data; 
  private $total;
  private $split;
  private $columnNames;
  private $columnTypes; 
  private $objectidPresent;

  //
  public $queue_table = "queue";

  /*  public $esriconvertable = array(
                    "esriFieldTypeSmallInteger" => "TINYINT",
                    "esriFieldTypeInteger"    => "INT",
                    "esriFieldTypeSingle"   => "FLOAT",
                    "esriFieldTypeDouble"   => "DOUBLE",
                    "esriFieldTypeString"   => "STRING",
                    "esriFieldTypeDate"     => "TIMESTAMP",
                    "esriFieldTypeOID"      => "INT",
                    "esriFieldTypeGeometry"   => "TEXT",
                    "esriFieldTypeBlob"     => "BLOB",
                    "esriFieldTypeRaster"   => "TEXT",
                    "esriFieldTypeGUID"     => "TEXT",
                    "esriFieldTypeGlobalID"   => "TEXT",
                    "esriFieldTypeXML"      => "TEXT");
  */

  // Convert esri response fields to postgis table datatypes
  public $esriconvertable = array(
                    "esriFieldTypeSmallInteger" => "TEXT",
                    "esriFieldTypeInteger"    => "TEXT",
                    "esriFieldTypeSingle"   => "TEXT",
                    "esriFieldTypeDouble"   => "TEXT",
                    "esriFieldTypeString"   => "TEXT",
                    "esriFieldTypeDate"     => "TEXT",
                    "esriFieldTypeOID"      => "TEXT",
                    "esriFieldTypeGeometry"   => "TEXT",
                    "esriFieldTypeBlob"     => "TEXT",
                    "esriFieldTypeRaster"   => "TEXT",
                    "esriFieldTypeGUID"     => "TEXT",
                    "esriFieldTypeGlobalID"   => "TEXT",
                    "esriFieldTypeXML"      => "TEXT",
                    "Latitude"      => "TEXT");

  function __construct($q){

    message("CSV: Starting a new arcgis item."); 

    $this->connection = drupal_connect();
    message("Connected to database: $this->connection");
    $this->q = $q; 
    $this->uuid = $q->array['uuid']; 
    $uuid = $this->uuid;
    message("Fetching data for uuid: $uuid");
    $this->id = $q->array['ID']; 
    message("Fetching data for queue id: $this->id");
    $this->tablename = "c".str_replace("-", "_", $this->uuid); 
    message("Fetching data for tablename: $this->tablename");
    
    $sql = "SELECT 
      n.uuid,
      n.nid,
      field_data_field_url.`field_url_url`
      FROM node n
      LEFT JOIN field_data_field_url ON n.nid = field_data_field_url.`entity_id`
      WHERE n.uuid = '$uuid'";
    $result = mysql_query($sql,$this->connection);
    
    while ($row = mysql_fetch_assoc($result)) {
      //message("******* *******");
      message(print_r($row));
      $this->url = $row["field_url_url"]; 
      message("Starting the process of node with uuid $this->tablename and url: $this->url");
    }
    
    if(empty($this->url)){
      $this->dbq_put('error', 'The url that should be processed is empty or doesn\'t exist'); 
      message("The url that should be processed is empty or doesn't exist. ($this->uuid)");

      return FALSE; 

    } else {
      message("Starting the process of file $this->url. ($this->uuid)");
      
      // start processing the url
      // Connect to postGIS
      $this->connection = columby_postgis_connect();

      // Clear existing data in queue. Each queue item starts from scratch. 
      $this->dbq_put("stats", NULL);
      $this->dbq_put("data", NULL);
      $this->dbq_put("total", NULL);
      $this->dbq_put("split", NULL);
      $this->dbq_put("error", NULL);
      $this->dbq_put("processing", 1);
      message("Processing columns queue table cleared. "); 


      // get stats
      message("************** GET stats: **************");
      $stats = $this->get_stats();
      if ($stats) {
        message("Stats received. "); 
        // put in database
        $this->dbq_put("stats", $stats);
        // put in local var, used for processing
        $this->stats = json_decode($stats);  

      } else {
        message("Did not receive any stats. "); 
        $this->dbq_put("processing","0");
        $this->dbq_put('error', 'Did not receive any stats. '); 

        return FALSE; // something went wrong, report back to render()
      }


      // get ids
      message("************** GET IDS: **************");
      $ids = $this->get_ids();
      if ($ids) {
        message("IDs Received. ");
        // put in database
        $this->dbq_put("data",$ids);  
        // put in local var, used for processing
        $this->data = $ids; 

      } else {
        message("Did not receive any IDs. ");
        $this->dbq_put("processing","0");
        $this->dbq_put('error', 'Did not receive any IDs. '); 

        return FALSE; // something went wrong, report back to render()
      }


      // get total
      message("************** GET TOTAL: **************");
      $total = $this->get_total();
      if ($total) {
        message("Totals received. "); 
        // put in database
        $this->dbq_put("total", $total);  
        // put in local var, used for processing
        $this->total = $total; 
      } else {
        message("Did not receive totals. ");
        $this->dbq_put("processing","0");
        $this->dbq_put("error", 'Did not receive total');

        return FALSE; // something went wrong, report back to render()
      }

      // Start the processing of all objects
      message("************** DBQ_GO: **************");
      $finished= $this->dbq_go();

      // Clear vars
      $this->stats = ''; 
      $this->data = ''; 
      $this->total = ''; 
      $this->split = ''; 

      $this->dbq_put("processing", 0); 

      if(!$finished){
        $this->dbq_put("processing","0");
        $this->dbq_put("error","Something went wrong in dbq_go...");
        message("*** ERROR *** putting queue item in state of error, processing set to 0");

        return FALSE; // something went wrong, report back to render()

      } else {

        $this->dbq_put("processing","0");
        $this->dbq_put("done","1");
        $this->dbq_put("error", NULL);
        message("FINISHED parsing: No errors, queue item set to DONE=1, processing=0");

        message("Constructing and saving file for this service. "); 
        $file = create_csv_file(); 
        // save file info into drupal. 


        return TRUE;

      };
    }
  }


  /** 
   * Main loop to process all items
   * 
   **/
  function dbq_go(){

    // Get number of items to process per run. 
    $cycle = $this->cycle;

    // Create an array of the id string, stored in data
    $ids = explode(",", $this->data);
    
    // Create chunks of remaining ids
    $chunks = array_chunk($ids,$cycle);
    
    // Process each chunk 
    // TODO Beter stukje erafhalen per keer? Wordt var steeds kleiner van ipv alles onthouden
    $chunkCount = count($chunks); 
    message("Starting the process of $chunkCount cycles of $cycle elements. "); 

    for ($currentChunk=0; $currentChunk<$chunkCount; $currentChunk++) {
      // Get records for this chunk
      $chunkNumber = $currentChunk;
      $chunkNumber++; 

      message("Getting records for chunk $chunkNumber (Records ". $currentChunk*$cycle . " to " . ($currentChunk*$cycle + $cycle) . ")");

      $records = $this->get_records(join($chunks[ $currentChunk],","));
      // get_records returns a string, convert to JSON object. 
      $records = json_decode($records);

      //print_r($records);

      if (!$records) {
        message("No records received for chunk $currentChunk");
        return FALSE;
      } else {
        // Records received
        // create columns and table at first run
        if ($currentChunk==0) {
          message("Creating a new table for this set. "); 
          $columnNames = []; 
          $columnTypes = [];
          
          // if ObjectID is not returned, add it as the first column. 
          $a = $records->fields;
          $idFound=false;
          foreach($a as $key){
            if ($key->name == 'OBJECTID') {
              $idFound = true; 
              message("*** Found ObjectID. ");
            } else { }
          }
          if ($idFound) {
            $this->objectidPresent = TRUE; 
          } else {
            message("*** ObjectID Not Found. "); 
            $columnNames[] = "OBJECTID";
            $columnTypes[] = "text";
            $this->objectidPresent = FALSE; 
            message('ObjectID is not returned, adding column.');  
          }
          

          // process each field 
          foreach($records->fields as $f){
            // convert esri types to postgis types
            $type = $this->esriconvertable[$f->type];
            $value = $f->name;
            // create column for the type
            $columnNames[] = $value;
            $columnTypes[] = $type;
          }
          //message(print_r($columnNames));
          //message(print_r($columnTypes));

          // add objectid, geometry and dates
          $columnNames[] = "the_geom";
          $columnTypes[] = "geometry";
          $columnNames[] = "createdAt";
          $columnTypes[] = "timestamp";
          $columnNames[] = "updatedAt";
          $columnTypes[] = "timestamp";

          $sqlcolumns = [];
          // Sanitize the column values;
          $columnNames = $this->sanitize_columns($columnNames); 

          // TODO check for equal length?
          for ($i=0; $i<count($columnNames); $i++) {
            $sqlcolumns[ $i] = $columnNames[ $i] . " " . $columnTypes[ $i]; 
          }
          // join columns into query string 
          $columnstring = join($sqlcolumns,",");
          
          // put in db
          $result = $this->pgdb_create_table($columnstring); // TODO with pgq() directly
          
          if ($result == FALSE) {
            message("Error creating table. "); 
            $this->dbq_put("processing","0");
            $this->dbq_put('error', "Error creating table. "); 

            // stop deze functie, false terug naar render()
            return FALSE;  

          } else {
            message('Table created: ' . $result); 
            $this->columnNames = $columnNames; 
            $this->columnTypes = $columnTypes; 
            message('Column names: ' . $columnstring); 
          }
        }

        message("Sending batch $currentChunk with " . count($chunks[$currentChunk]) . " id's to insert_rows. "); 

        // Send the results to the insert to database function
        // TODO Also send the array with ids we are working with...
        $result = $this->pgdb_insert_rows($records, $chunks[ $currentChunk]);

        message("MEMORY USAGE: ".hr_memory_usage());
        message("result pgdb_insert_rows: $result"); 
      }
    } 

    message ("Processed all chunks. "); 
    return TRUE; 
  }

  /** 
   * Add rows to postGIS database
   * 
   * Input: Records( json object with all data), Ids( Array with all objectIds matchin the Records)
   * Output: TRUE or FALSE
   *
   **/
  function pgdb_insert_rows($records, $current_ids){
    
    $recordCount = count($records->features); 
    $idCount = count($current_ids); 
    message("Records: $recordCount, idCount: $idCount. "); 

    //check if records and currentIds count match .. .
    if ( $recordCount != $idCount) {
      message(print_r($records,true));
      message("Error: Records ($recordCount) and current Ids ($idCount) do not match. "); 
      return FALSE; 
    }

    // array for all value rows. 
    $valueLines = []; 

    // Process each feature
    for($i=0; $i<$recordCount; $i++){
      
      if($records->features[$i]){

        $values = array_values(get_object_vars($records->features[$i]->attributes));
        
        // add objectID as the first item in the array if it is not returned from the service. 
        //message("*** ObjectID Present: "); 
        //message($this->objectidPresent); 
        if (!$this->objectidPresent) {
          //message("OBJECTID is not returned by the service, using the current_id. "); 
          array_unshift($values, $current_ids[$i]);
        }

        // Escape input values
        foreach($values as $k => $v){
          $v = is_null($v) ? "null" : "'". pg_escape_string($v) ."'";
          $escaped_values[$k] = $v;
        }
        $values = $escaped_values; 

        // Get geometry data from response
        $g = get_object_vars($records->features[$i]->geometry);
        // get the keys
        $gk = array_keys($g); 
        // get the values
        $gv = array_values($g); 
        // wkt POINT(lon lat) == POINT(x y) == POINT(4.3 52.0)
        //message(print_r($gk['rings']));
        if (($gk[0] == 'x') && ($gk[1] == 'y')) {
          $wkt = "ST_GeomFromText('POINT(" . $gv[0] . " " . $gv[1] .")', 4326)"; 
          $values[] = $wkt; 
        } elseif($gk[0] == 'rings') {
          /*
          "features": [{
              "attributes": {
                "BUURTCODE": "44"
              },
              "geometry": {
                "rings": [
                  [
                    [
                      78889.131000001,
                      455079.796
                    ],
                    [
                      78914.4580000006,
                      455092.798
                    ],
          */
          $rings = $gv[0]; 
          $items = $rings[0];
          $pointArray = []; 
          for ($j=0; $j<count($items); $j++) {
            $point = $items[$j]; 
            $pointArray[] = $point[0] . " " .  $point[1];
          }
          $pointString = implode(",", $pointArray); 
          $wkt = "ST_GeomFromText('POLYGON((" . $pointString . "))', 4326)";
          $values[] = $wkt;
        } else {
          $values[] = NULL;
        }

        // process dates
        date_default_timezone_set('UTC');
        $values[] = "'" . date('Y-m-d H:i:s') . "'";  // createdAt
        $values[] = "'" . date('Y-m-d H:i:s') . "'";  // updatedAt

        //message(print_r($values)); 

        // Create string from escaped values
        $values  = implode(", ", $values);

        $valueLines[] = "(" . $values . ")"; 
      }
    }
    
    $valueLines = implode(", ", $valueLines); 
    //message("valueLines: " . $valueLines); 

    $columns = implode(", ", $this->columnNames);

    $sql = "INSERT INTO $this->tablename ($columns) VALUES $valueLines;";
    
    //message("Sending sql: $sql"); 
    
    $q = pgq($sql);
    if ($q == FALSE) {
      message("Error saving "); 
    } else {
      message("Saved values to postGIS"); 
    }
    
    $this->split += $recordCount; 
    $this->dbq_put("split",$this->split);
    $percentage = round($this->split/($this->total/100),2);
    $this->dbq_put("processed","$percentage");
    message("*** ------ Insert update information: ------ "); 
    message("*** $recordCount inserts for " . $this->uuid . " ::: $percentage% ($this->split of $this->total)");
    
    return TRUE; 
  }

  /** 
   * Create a postgis table
   * 
   * @param string
   *   with columns
   * 
   * @return string
   *   string with result (json form) or FALSE
   **/ 
  function pgdb_create_table($columns){

    $sql="CREATE TABLE IF NOT EXISTS $this->tablename (cid serial PRIMARY KEY, $columns);"; 
    message("Creating table: $sql"); 
    $q = pgq($sql); 

    return $q; 
  }

  // check input value? 
  function dbq_put($col,$val){
    $table = $this->q->queue_table;
    $id = $this->q->array['ID'];
    $val = addslashes($val); 
    mysql_query("UPDATE $table SET $col='$val' WHERE ID='$id'",$this->q->connection) or die(mysql_error());
    $this->q->array[$col] = $val;

    message("Queue column '$col' updated to '" . substr($val, 0, 100) . " (sample)"); 
  }

  function db_close(){
    mysql_close($this->connection);
  }


  /** 
   * Get stats from a given q->url
   * 
   * @return string 
   *   String with json result or FALSE
   **/
  function get_stats(){
    message("Getting stats... ");

    $params = array(
      "f" => "json",
      "pretty" => "true"
    );

    $stats = $this->get($params);
    $json = json_decode($stats);
    $this->version = $json->currentVersion;
    
    if ($stats == FALSE) {
      return FALSE; 
    } else {
      return $stats; 
    }
  } 


  /** 
   * Get all Object IDs from a given q->url
   * 
   * @return string
   *   String with ids or FALSE
   **/
  function get_ids(){

    message("get all objectIds.");

    $params = array(
      "f"             => "pjson",
      "objectIds"     => "",
      "where"         => "1=1",
      "returnIdsOnly" => "true",
      //"outFields"   => "",
      "text"          => "",
      "returnGeometry"  => "false"
    );

    $ids = $this->get($params,"/query");
    
    if ($ids == FALSE) {
      
      return FALSE; 

    } else {
      $ids = json_decode($ids);
      $arr = $ids->objectIds;
      $ids = join($arr,",");
      
      return $ids; 
    }
  }

  /** 
   * Get number of objects from a given q->url
   * 
   * @return 
   *   String with count or FALSE
   **/
  function get_total(){

    $params = array(
      "f"         => "pjson",
      "objectIds"     => "",
      "where"       => "1=1",
      "returnIdsOnly"   => "true",
      "returnCountOnly" => "true",
      "text"        => "",
      "returnGeometry"  => "false"
    );

    $total = $this->get($params,"/query");
    if($total == FALSE) {
      return FALSE; 
    } else {
      $result = json_decode($total);
      return $result->count; 
    }
  }

  /**
   * Get record information of id's
   * 
   * @param string 
   *   comma separated list of ids
   * 
   * @return string 
   *   String with json or FALSE; 
   **/
  function get_records($ids){

    message("Getting record information for arcGIS service version: " . $this->stats->currentVersion);

    if($this->stats->currentVersion=="10.04"){
      $params = array(
        "f"               => "pjson",
        "where"           => "1=1",
        //"time"          => "$begin,$eind",
        "returnIdsOnly"   => "false",
        "text"            => "",
        "returnGeometry"  => "true",
        "geometryType"    => "esriGeometryEnvelope",
        "spatialRel"      => "esriSpatialRelIntersects",
        "outFields"       => "*",
        "outSR"           => "4326", 
        "objectIds"       => "$ids"
      );
    } else if($this->stats->currentVersion=="10.11"){
      $params = array(
        "f"               => "pjson",
        "where"           => "1=1",
        //"time"          => "$begin,$eind",
        "returnIdsOnly"   => "false",
        "text"            => "",
        "returnGeometry"  => "true",
        "geometryType"    => "esriGeometryEnvelope",
        "spatialRel"      => "esriSpatialRelIntersects",
        "outFields"       => "*",
        "outSR"           => "4326", 
        "objectIds"       => "$ids"
      );
    } else {
      return FALSE;
    }

    $result = $this->get($params,"/query");

    if ($result) {
      return $result; 
    } else {
      return FALSE; 
    } 
  }

  /**
   * Get the contents from the given q url
   *
   * @param array $params
   *   parameters for the url query
   * 
   * @return string
   *   JSON result as String, or FALSE when error
   **/
  function get($params,$pre=""){

    // join for GET request
    $q = [];
    foreach($params as $k=>$v){
      $q[] = "$k=$v";
    };
    $q = "$pre?".join($q,"&");

    // print query always to monitor external connections
    $sample = strval(substr($this->url.$q, 0, 150));
    message("external server query: ".$sample);

    $result = file_get_contents($this->url.$q);

    message("return type: " . gettype($result)); 
    //message("Result: $result");
    $sample = strval(substr($result, 0, 100)); 
    message("getting: $sample [...]"); 

    return $result; 
  }

  function sanitize_columns($columns) {
    message(print_r($columns));
    $fields = array(); 
    foreach ($columns as $field) {
      $field = str_replace(' ', '_', $field);
      $field = str_replace('.', '_', $field);
      $field = strtolower($field);
      $fields[]=$field;
    }
    //message("Sanitized columns: ");
    //message(print_r($fields));

    return $fields;
  }


  /**
   * Create a CSV file from the table. Used at the end of the processing cycle.
   * @return array
   *   array with file details (path, name)
   **/
  function create_csv_file() {
    /*
    cd /usr/local/pgsql
    mkdir csv
    chown postgres csv
    chmod 755 csv
    ls -la
    
    COPY tablename
    TO '/usr/local/pgsql/data/csv/header_export.csv'
    WITH DELIMITER ‘,’
    CSV HEADER
    */
    
    // create a tmp file
    //$file = fopen("home/arn/www/sites/default/files/datasets/tmp/$uuid_tmp.csv", "w"); 
    // add headers

    // query, on success, save results to file 
    //this.uuid = $("#table-js").attr("uuid").replace(new RegExp("-","gm"),"_");
    //queryurl = deze.root+'api/data/'+uuid+'/schema.json'

    // save file permanently 

    // attach to node
  }
}
?>