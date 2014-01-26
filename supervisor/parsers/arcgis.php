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
  private $stats;             // stats received from the service
  private $ids;               // List of received id's 
  private $total;             // Total number of items received
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

    // Safe to assume all arcGIS data is geo.
    $this->geo = '1'; 

    $l = get_log(); 
    message('log:','log','DEBUG');
    message(implode(",",$l),'log','DEBUG');

    message("ArcGIS: Starting a new arcgis item.", 'log','NOTICE'); 

    $this->connection = drupal_connect();
    if (!$this->connection){
      message("ArcGIS: Error connecting to the job queue database. ",'log','ERROR');

    } else {

      message("ArcGIS: Connected to the job queue database. ",'log','NOTICE');
      $this->job = $job; 
      $this->tablename = "c".str_replace("-", "_", $job['uuid']); 
      $this->id = $job['ID'];
      $this->uuid = $job['uuid'];

      message("ArcGIS: Fetching data for uuid: " . $job['uuid'],'log','NOTICE');
      message("ArcGIS: Fetching data for queue id: " . $job['ID'],'log','NOTICE');
      message("ArcGIS: Fetching data for tablename: " . $this->tablename,'log','NOTICE');

      $sql = "SELECT 
        n.uuid,
        n.nid,
        field_data_field_url.`field_url_url`
        FROM node n
        LEFT JOIN field_data_field_url ON n.nid = field_data_field_url.`entity_id`
        WHERE n.uuid = '" . $this->uuid ."'";
      
      $result = mysql_query($sql,$this->connection);

      // Process results
      if (!$result) {
        message("ArcGIS: Error Fetching data from CMS: " . mysql_error(),'log','ERROR');
      } else {

        $row = mysql_fetch_assoc($result);
        $this->url = $row["field_url_url"]; 
        message("ArcGIS: Starting the process of node with uuid $this->uuid and url: $this->url. ",'log','NOTICE');
        
        if(empty($this->url)){
          message("ArcGIS: The url that should be processed is empty or doesn\'t exist. ",'log','ERROR');
        } else {
          // start processing the csv-file
          $this->process();
        }
      }

      // End of processing data, update status
      $el = get_errorlog();
      if (count($el) > 0) {
        $el = implode("\n", $el);
        $this->update_queue('error', $el);
      }
      $this->sync_date = date('Y-m-d H:i:s', strtotime('now'));
      
      $this->update_queue('done', "1");
      $this->update_queue('processing', "0");
    }
  }


  /**
   * Process the arcGIS url
   **/
  function process(){
      
    // start processing the url
    
    // get stats
    message("************** GET stats: **************",'log','NOTICE');
    $stats = $this->get_stats();
    if ($stats) {
      message("Stats received. ",'log','NOTICE'); 
      // put in database
      $this->update_queue("stats", $stats);
      // put in local var, used for processing
      $this->stats = json_decode($stats);

    } else {
      message("Did not receive any stats. ",'log','ERROR');
      return FALSE; // something went wrong, report back to render()
    }


    // get ids
    message("************** GET IDS: **************",'log','NOTICE');
    $ids = $this->get_ids();
    if ($ids) {
      message("IDs Received. ",'log','NOTICE');
      // put in database
      $this->update_queue("data",$ids);  
      // put in local var, used for processing
      $this->ids = explode(',', $ids);
    } else {
      message("Did not receive any IDs. ",'log','ERROR');
      return FALSE; // something went wrong, report back to render()
    }


    // get total
    message("************** GET TOTAL: **************",'log','NOTICE');
    $total = $this->get_total();
    if ($total) {
      message("Totals received. ",'log','NOTICE');
      // put in database
      $this->update_queue("total", $total);  
      // put in local var, used for processing
      $this->total = $total; 
    } else {
      message("Did not receive totals. ",'log','NOTICE');
      return FALSE; // something went wrong, report back to render()
    }

    // Create table
    message("************** CREATING TABLE **************",'log','NOTICE');
    $db = $this->create_table(); 
    if ($db){
      message("Table created. ",'log','NOTICE');
    } else {
      message("Error creating table.",'log','ERROR');
      return FALSE;
    }

    // Start the processing of all objects
    message("************** STARTING SYNC **************",'log','NOTICE');
    
    $finished= $this->sync();

    if(!$finished){
      message("*** ERROR *** putting queue item in state of error",'log','ERROR');
      return FALSE; // something went wrong, report back to render()
    } else {
      message("FINISHED parsing: No errors.",'log','NOTICE');
      return TRUE;
    };
  } 


  /** 
   * Main loop to process all items
   * 
   **/
  function sync(){

    // Get number of items to process per run. 
    $cycle = $this->cycle;
    $cycles = ceil(count($this->ids)/$cycle);
    message("Starting the sync of $cycles cycles of $cycle elements. ",'log','NOTICE');

    $counter=0;
    while (count($this->ids)>0){
      $counter++;
      $idList = array_splice($this->ids, 0 , $cycle); // ($array,offset,length)

      message("Getting records for cycle $counter out of $cycles (Records ". ($counter -1)*$cycle . " to " . (($counter)*($cycle)) . ")",'log','NOTICE');
      message("ArcGIS: items left in queue: ".count($this->ids),'log','NOTICE');

      $ids = join($idList, ",");
      message('ArcGIS: Getting ids: ' . $ids,'log','NOTICE');
      $records = $this->get_records($ids);

      // get_records returns a string, convert to JSON object. 
      $records = json_decode($records);

      if (!$records) {
        message("No records received for chunk $currentChunk",'log','ERROR');
        return FALSE;
      } else {
        message("Sending cycle $counter with " . count($records->features) . " id's to insert_rows. ",'log','NOTICE');

        $result = $this->process_records($records, $idList);
        if ($result == FALSE) {
          message("Error during syncing. Stopping the process.",'log','NOTICE');
          return FALSE;
        }
        message("MEMORY USAGE: ".hr_memory_usage(),'log','NOTICE');
        message("Sync result: $result",'log','NOTICE');
      }
    } 

    message("Processed all ids. ",'log','NOTICE');

    return TRUE; 
  }


  function create_table(){
    message("Creating a new table for this set. ",'log','NOTICE');
    
    $stats = $this->stats; 
    foreach($stats->fields as $f){
      
      // convert esri types to postgis types
      $this->columns['"' . $f->name . '"'] = $this->esriconvertable[$f->type];
      
      // check for objectid
      if (strtolower($f->name) == 'objectid') {
        $this->objectidPresent = TRUE;
        message("Found ObjectID. ",'log','NOTICE');
      }
    }

    if(!$this->objectidPresent) {
      message("ObjectID Not Found, adding it to columns. ",'log','NOTICE');
      // Add objectID to beginning of array
      $this->columns['"OBJECTID"'] = "text";
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
    $sql="CREATE TABLE IF NOT EXISTS $this->tablename (cid serial PRIMARY KEY, $columnstring);"; 
    message("Creating table: $sql",'log','NOTICE');
    
    $pgResult = postgis_query($sql); 
    
    if ($pgResult) {
      message("Table created.",'log','NOTICE');
      $this->columnNames = $fields;
      return TRUE;
    } else {
      message('Error creating table.','log','NOTICE');
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
    
    $recordCount = count($records->features); 
    $idCount = count($idList); 

    //check if records and currentIds count match .. .
    if ( $recordCount != $idCount) {
      message("Error: Records ($recordCount) and current Ids ($idCount) do not match. ",'log','NOTICE');
      return FALSE; 
    }

    message("Starting the process of $recordCount records. ",'log','NOTICE'); 

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
    message("Constructed SQL: " . $sql,'log','DEBUG');

    $pgResult = postgis_query($sql);
    if ($pgResult) {
      message("Saved values to postGIS",'log','NOTICE');
    } else {
      message("Error saving ",'log','ERROR');
      return FALSE;
    }
    
    $this->split += $recordCount; 
    $this->update_queue("split",$this->split);
    $percentage = round($this->split/($this->total/100),2);
    $this->update_queue("processed","$percentage");
    message("*** ------ Insert update information: ------ ",'log','NOTICE');
    message("*** $recordCount inserts for " . $this->uuid . " ::: $percentage% ($this->split of $this->total)",'log','NOTICE');
    
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
      $wkt = "ST_GeomFromText('POLYGON((" . $pointString . "))', 4326)";

      message (print_r($geometry),'log','NOTICE');

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

      message('IATI: Found polylines (linestring)','log','message, service, severity');
      //message(print_r($geometry['paths']),'log','DEBUG');

      $lines=[]; 
      // Process each line (mostly one, but could be multiple)
      foreach ($geometry['paths'] as $key => $line) {
        message("IATI: Polyline $key",'log','DEBUG');
        //message(print_r($line),'log','DEBUG');
        $points=[];
        // Process each point in the line
        foreach ($line as $k => $point) {
          //message("IATI: Point $key:",'log','DEBUG');
          //message(print_r($point),'log','DEBUG');
          // convert point array to string
          $point = implode(' ', $point);
          message("IATI: Point $k value: $point",'log','DEBUG');
          $points[] = $point;
        }
        // create linestring from points array
        $points = "(" . implode(',', $points) . ")";
        $lines[] = $points;
        message("IATI: Linestring value: $points",'log','DEBUG');
      }
      // convert linestring array to string
      $lines = implode(',', $lines);
      message("IATI: Lines: $lines",'log','DEBUG');
      $wkt = "ST_GeomFromText('MULTILINESTRING(" . $lines . ")', 4326)";
      message("IATI: wkt: $wkt",'log','DEBUG');      

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
  function update_queue($col,$val){
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
    message("Getting stats... ",'log','NOTICE');

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

    message("get all objectIds.",'log','NOTICE');

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

    message("Getting record information for arcGIS service version: " . $this->stats->currentVersion,'log','NOTICE');

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
    message("Server query: ".$this->url.$q,'log','DEBUG');

    $result = file_get_contents($this->url.$q);

    message("return type: " . gettype($result),'log','DEBUG');
    //message("Response result: $result",'log','DEBUG');

    return $result; 
  }

  function sanitize_columns($columns) {
    message(print_r($columns),'log','NOTICE');
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