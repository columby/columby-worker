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

  public $geo;           // is the set geodata? 
  public $worker_error;   // Text with error to send to API
  public $worker_status;  // Status of the worker
  public $sync_date;

  // Number of items to query per request
  public $cycle = 100;
  
  private $connection;
  private $uuid;
  private $tablename;
  private $stats;             // String stats received from the service
  private $ids;               // array  List of received id's 
  private $total;             // int    Total number of items received
  private $split;             // Current position of the processor
  private $columns;           // Associative array of column name and type
  private $objectidPresent;   // Boolean if objectID is present in the service

  private $queue_table = "columby_queue";

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

  function __construct($job){

    global $logger; 
    global $error;
    global $errors;

    // Safe to assume all arcGIS data is geo.
    $this->geo = '1'; 
    $logger->add("--- ARCGIS PARSER INITIATED --------------------------------------------------------- ");

    $this->connection = drupal_connect();
    if (!$this->connection){
      $logger->add("Error connecting to the job queue table.");
      $error = true; 
      $errors[] = "Error connecting to the job queue table.";
      return FALSE; 
    }

    $logger->add("Connected to the job queue database. ");
    $this->job = $job; 
    $this->tablename = "c".str_replace("-", "_", $job['uuid']); 
    $this->id = $job['ID'];
    $this->uuid = $job['uuid'];

    $sql = "SELECT 
      n.uuid,
      n.nid,
      field_data_field_url.field_url_url
      FROM node n
      LEFT JOIN field_data_field_url ON n.nid = field_data_field_url.entity_id
      WHERE n.uuid = '" . $this->uuid ."'"
    ;
    
    $result = mysql_query($sql,$this->connection);

    // Process results
    if (!$result) {
      $logger->add("ArcGIS: Error Fetching data from CMS: " . mysql_error());
      $error = TRUE; 
      $errors[] = "ArcGIS: Error Fetching data from CMS: " . mysql_error();
      return FALSE; 
    } 

    $row = mysql_fetch_assoc($result);
    $this->url = $row["field_url_url"]; 
    $logger->add("Starting the process of node with uuid $this->uuid and url: $this->url.");
      
    if(empty($this->url)){
      $logger->add("The url that should be processed is empty or doesn\'t exist. ");
      $error = TRUE; 
      $errors[] = "ArcGIS: Error Fetching data from CMS: " . mysql_error();
      return FALSE;
    }

    // start processing the csv-file
    $this->process();
    
    $logger->add("Finished processing arcgis");

    // End of processing data, update status
    if (count($errors) > 0) {
      $errormsg = implode("\n", $errors);
      $this->update_queue('error', $errormsg);
      echo "Errors: " . $errormsg;
    }

    $this->update_queue('done', "1");
    $this->update_queue('processing', "0");
    
    return TRUE;
  }


  /**
   * Process the arcGIS url
   **/
  function process(){
    global $logger, $error, $errors; 

    // get stats
    $logger->add("--- ARCGIS: GET STATS ----------------------------------------------------------------- ");
    $stats = $this->get_stats();
    if ($stats) {
      $logger->add("Stats received. "); 
      $json = json_decode($stats);
      $this->version = $json->currentVersion;
      // put in database
      $this->update_queue("stats", $stats);
      // put in local var, used for processing
      $this->stats = json_decode($stats);
    } else {
      $logger->add("Did not receive any stats. ");
      $error = TRUE;
      $errors[] = "Did not receive any stats. ";
      return FALSE; // something went wrong, report back to render()
    }


    // get ids
    $logger->add("--- ARCGIS: GET ID's ----------------------------------------------------------------- ");
    $id_array = $this->get_ids();
    if ($id_array) {
      $total = count($id_array);
      $this->total = $total;
      $id_string = implode(',',$id_array);
      $this->ids = $id_array; 

      $logger->add("Received " . $total . " ID's");

      // put in database
      $this->update_queue("data", $id_string);
      $this->update_queue("total", $total);
      
    } else {
      $logger->add("Did not receive any ID's. ");
      $error = TRUE;
      $errors[] = "Did not receive any ID's. ";
      return FALSE; // something went wrong, report back to render()
    }

    // Create table
    $logger->add("--- ARCGIS: CREATE TABLE ----------------------------------------------------------------- ");
    $db = $this->create_table(); 
    if ($db){
      $logger->add("Creating table finished. Next step.");
    } else {
      $logger->add("Error creating table.");
      $error = TRUE;
      $errors[] = "Error creating table.";
      return FALSE;
    }

    // Start the processing of all objects
    $logger->add("--- ARCGIS: Synchronise ----------------------------------------------------------------- ");
    $finished = $this->sync();
    // Check the status of the synchronisation. 
    if(!$finished){
      $logger->add("There was an error during synchronisation.");
      $error = TRUE;
      $errors[] = "There was an error during synchronisation.";
      return FALSE;
    } else {
      $logger->add("Succesfully finished parsing the feed. ");
      return TRUE;
    };
  } 


  /** 
   * Main loop to process all items
   * 
   **/
  function sync(){
    global $logger;
    global $errors;

    // Get number of items to process per run. 
    $cycle = $this->cycle;
    $total = count($this->ids);
    $cycles = ceil($total/$cycle);
    $logger->add("Starting the synchronisation of $total elements: $cycles cycles of $cycle elements. ");

    $counter=0;
    while (count($this->ids)>0){
      $counter++;
      // Create a separate array with the id's to process.
      // id's are substracted from main id array
      $id_list = array_splice($this->ids, 0 , $cycle);

      if (count($id_list)<1){
        $logger->add("No id's present in the list. ");
        return FALSE;
      }
      
      // sometimes objectid 0 is returned. This gives troubles, so removing it.. 
      $id_list = array_diff($id_list, array('0'));

      $logger->add("Fetching records for cycle $counter out of $cycles (Records ". ($counter -1)*$cycle . " to " . (($counter)*($cycle)) . ")");
      $logger->add("Number of objects left in queue: " . count($this->ids));

      $ids = join($id_list, ",");
      $logger->add("Getting data for " . count($id_list) . " object-id's: " . $ids);

      $records = $this->get_records($ids);

      // get_records returns a string, convert to object. 
      $records = json_decode($records);

      if (!$records) {
        $logger->add("No records received for cycle $counter");
        $errors[] = "No records received for cycle $counter";
        return FALSE;
      }

      $logger->add("Received " . count($records->features) . " records for cycle $counter.  Sending the records to the database.");
      if (count($records->features) == count($id_list)) {
        $result = $this->process_records($records, $id_list);
        if ($result == FALSE) {
          $logger->add("Error during syncing. Stopping the process.");
          return FALSE;
        }
      } else {
        $logger->add("The number of received records (" . count($records->features) . ") does not match the number of requested records (" . count($id_list) . ")"); 
        //return FALSE;
      }
        
      $logger->add("MEMORY USAGE: ".hr_memory_usage());
      $logger->add("Sync result: $result");
    }

    $logger->add("Processed all IDs. ");

    return TRUE; 
  }


  function create_table(){
    global $logger; 

    $logger->add("Creating a new table for this set. ");
    
    $stats = $this->stats; 

    // clear existing columns to be sure.
    $this->columns = [];
    // Check if there are fields available. 
    $fields = $stats->fields; 
    if (count($fields)<1){
      $logger->add('No fields found. ');
      return FALSE;
    }

    foreach($fields as $f){
      // convert esri types to postgis types
      $this->columns['"' . $f->name . '"'] = $this->esriconvertable[$f->type];
      $logger->add("Added column '" . $f->name . "' with type '" . $this->columns['"' . $f->name .'"'] . "'.");

      // Check if objectID is supplied by the service
      if (strtolower($f->name) == 'objectid') {
        $this->objectidPresent = TRUE;
        $logger->add("ObjectID is supplied by the service.");
      }
    }

    if(!$this->objectidPresent) {
      $logger->add("ObjectID not supplied by the service, adding it to the columns");
      $this->columns['"OBJECTID"'] = "text";
      $logger->add("Added column 'OBJECTID' with type 'text'.");
    }

    $this->columns['the_geom'] = "geometry"; 
    $this->columns['"createdAt"'] = "timestamp";
    $this->columns['"updatedAt"'] = "timestamp";

    $fields = array();
    foreach ($this->columns as $key => $value) {
      $fields[] = $key . " " . $value;
    }

    // join columns into query string 
    $columnstring = join($fields,",");
    $logger->add("Creating table with name: " . $this->tablename . " and columns: " . $columnstring . ".");
    
    $sql="CREATE TABLE IF NOT EXISTS $this->tablename (cid serial PRIMARY KEY, $columnstring);"; 
    
    $pgResult = postgis_query($sql); 
    
    if ($pgResult) {
      $logger->add("New table created.");
      $this->columnNames = $fields;
      return TRUE;
    } else {
      message('There was an error creating the table.');
      return FALSE; 
    }
  }

  /** 
   * Add rows to postGIS database
   * 
   * Input: Records( json object with all data), Ids( Array with all objectIds matchin the Records)
   * Output: TRUE or FALSE
   *
   **/
  function process_records($records, $idList){
    
    global $logger; 

    $recordCount = count($records->features); 
    $idCount = count($idList); 

    //check if records and currentIds count match .. .
    if ( $recordCount != $idCount) {
      $logger->add("Error: Records ($recordCount) and current Ids ($idCount) do not match.");
      return FALSE; 
    }

    $logger->add("Starting the process of $recordCount records."); 

    // array for all value rows. 
    $keys = [];
    $values = []; 

    // Process each feature (row)
    for($i=0; $i<count($records->features); $i++){
      if($records->features[$i]){
        // get keyed array of values
        $a = $this->process_data($records->features[$i], $idList[$i]);
        // get keys from first feature
        if ($i==0){
          $keys = array_keys($a);
          // escape keys and values
          foreach ($keys as $key=>$value) {
            $keys[$key] = '"'.$value.'"';
          }
        }
        // escape values except 'NULL' and the_geom column
        foreach ($a as $key => $value) {
          if (($value != 'NULL') && ($key != 'the_geom')) {
            $escaped_value = pg_escape_string($value);
            $a[$key] = "'". $escaped_value ."'";
          }
        }
        $values[] = "(" . implode(",",array_values($a)) . ")"; 
      } else {
        //message('feature does not exist','log','NOTICE');
      }
    }

    $sql = 'INSERT INTO ' . $this->tablename . ' (' . implode(',',$keys) . ") VALUES " . implode(',',$values) . ";";
    echo "Constructed SQL: " . $sql;

    $pgResult = postgis_query($sql);
    if ($pgResult) {
      $logger->add("Saved values to the table");
    } else {
      $logger->add("Error saving values to the table");
      return FALSE;
    }
    
    $this->split += $recordCount; 
    $this->update_queue("split",$this->split);
    $percentage = round($this->split/($this->total/100),2);
    $this->update_queue("processed","$percentage");
    $logger->add("Insert update information: ");
    $logger->add("$recordCount inserts for " . $this->uuid . " ::: $percentage% ($this->split of $this->total)",'log','NOTICE');
    
    return TRUE; 
  }

  function process_data($record, $id){
    
    //initialize data
    $attributes = get_object_vars($record->attributes);
    $attribute_keys = array_keys($attributes); 
    $attribute_values = array_values($attributes);    

    //message('Data keys: ' . implode(',',$attribute_keys),'log','DEBUG');
    
    // add objectID as the first item in the array if it is not returned from the service. 
    if (!array_key_exists('OBJECTID',$attributes)) {
      //message("OBJECTID is not returned by the service, using the current_id.",'log','DEBUG');
      $attributes['OBJECTID'] = $id;
    } else {
      //message("OBJECTID is returned by the service.",'log','DEBUG');
    }

    // Escape input values
    foreach($attributes as $key=>$value){
      $value = is_null($value) ? "null" : pg_escape_string($value);
      $attributes[$key] = $value;
    }
    
    // Process geometry
    $geometry = get_object_vars($record->geometry);
    $geometry_keys = array_keys($geometry); 
    $geometry_values = array_values($geometry); 
    // wkt POINT(lon lat) == POINT(x y) == POINT(4.3 52.0)
    if (($geometry_keys[0] == 'x') && ($geometry_keys[1] == 'y')) {
      $wkt = "ST_GeomFromText('POINT(" . $geometry_values[0] . " " . $geometry_values[1] .")', 4326)"; 
    } elseif($geometry_keys[0] == 'rings') {
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

      $rings = $geometry_values[0]; 
      $items = $rings[0];
      $pointArray = []; 
      for ($j=0; $j<count($items); $j++) {
        $point = $items[$j]; 
        $pointArray[] = $point[0] . " " .  $point[1];
      }
      $pointString = implode(",", $pointArray); 
      // 2 points is a linestring, more points is a polygon
      if (count($pointArray)==2){
        $wkt = "ST_GeomFromText('LINESTRING(" . $pointString . ")', 4326)";
      } else if( $pointArray[0] != $pointArray[count($pointArray)-1] ) {
        // when first and last points match it is a polygon
        $wkt = "ST_GeomFromText('LINESTRING(" . $pointString . ")', 4326)";
      } else {
        $wkt = "ST_GeomFromText('POLYGON((" . $pointString . "))', 4326)";
      }
    } elseif ($geometry_keys[0] == 'points') {
      
      message('found a multipoint','log','NOTICE');
      message('geometry values: '. implode(',',$geometry['points'][0]),'log','NOTICE');
      
      $point = $geometry_values['points'][0];
      message('point' . implode(',',$geometry['points'][0]),'log','NOTICE');
      
      $wkt = "ST_GeomFromText('POINT(" . implode(' ',$geometry['points'][0]) . ")', 4326)";
      message('wkt' . $wkt,'log','NOTICE');

    } elseif ($geometry_keys[0] == 'paths') {
      /*
      Array(
        [0] => Array(
          [0] => Array(
            [0] => 4.2329000622957
            [1] => 52.059222035529
          )
          [1] => Array(
            [0] => 4.2318033708134
            [1] => 52.059160187838
          )
          [2] => Array(
            [0] => 4.2318008523102
            [1] => 52.059160010292
          )
        )
      )
      */

      $lines=[]; 
      // Process each line (mostly one, but could be multiple)
      foreach ($geometry['paths'] as $key => $line) {
        //message(print_r($line),'log','DEBUG');
        $points=[];
        // Process each point in the line
        foreach ($line as $k => $point) {
          //message("IATI: Point $key:",'log','DEBUG');
          //message(print_r($point),'log','DEBUG');
          // convert point array to string
          $point = implode(' ', $point);
          $points[] = $point;
        }
        // create linestring from points array
        $points = "(" . implode(',', $points) . ")";
        $lines[] = $points;
      }
      // convert linestring array to string
      $lines = implode(',', $lines);
      $wkt = "ST_GeomFromText('MULTILINESTRING(" . $lines . ")', 4326)";
      
    } else {
      message('Found a geometry field, but unable to process.','log',"NOTICE");
      $wkt = 'NULL';
    }
    $attributes['the_geom'] = $wkt;
    //message('Added geometry: ' . $wkt, 'log','DEBUG');

    // process dates
    date_default_timezone_set('UTC');
    $attributes['createdAt'] = date('Y-m-d H:i:s');
    $attributes['updatedAt'] = date('Y-m-d H:i:s');
    
    return $attributes;
  }  

  // check input value? 
  function update_queue($col,$val=""){
    $id = $this->job['ID'];
    $val = addslashes($val); 
    mysql_query("UPDATE columby_queue SET $col='$val' WHERE ID='$id'",$this->connection) or die(mysql_error());
    $this->q->array[$col] = $val;

    message("Queue column '$col' updated to '" . substr($val, 0, 100) . " (sample)",'log','NOTICE');
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
    global $logger;

    $logger->add("Getting stats.");

    $params = array(
      "f" => "json",
      "pretty" => "true"
    );

    $stats = $this->get($params);
    
    return $stats;
  }


  /** 
   * Get all Object IDs from a given q->url
   * 
   * @return string
   *   String with ids or FALSE
   **/
  function get_ids(){
    global $logger; 

    $logger->add("Fetching all objectIds.");

    $params = array(
      "f"             => "pjson",
      "objectIds"     => "",
      "where"         => "1=1",
      "returnIdsOnly" => "true",
      "text"          => "",
      "returnGeometry"  => "false"
    );

    $ids = $this->get($params,"/query");
    
    if ($ids) {
      $ids = json_decode($ids);
      $ids = (array)$ids->objectIds; 
    }

    return $ids; 
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
    global $logger; 

    $logger->add("Getting record information for arcGIS service version: " . $this->stats->currentVersion);

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

    $result = @file_get_contents($this->url.$q);
    
    return $result; 
  }

  function sanitize_columns($columns) {
    //message(print_r($columns),'log','NOTICE');
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
}
?>