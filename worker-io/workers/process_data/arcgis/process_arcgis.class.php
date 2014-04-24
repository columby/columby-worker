<?php 

/*
Bomenservice  cycle=100 32 minuten = 27.100 bomen
              cycle=250 28 minuten = 49.750 bomen
*/

class ProcessArcGIS {

  // Class variables
  var $uuid;
  var $data_url;
  var $config;
  var $postgis_tablename;
  var $sync_date;

  var $geo;
  var $cycle = 250;
  var $ids;               // array  List of received id's 
  var $total;             // int    Total number of items received
  var $split;             // Current position of the processor
  var $columns;           // Associative array of column name and type
  var $objectidPresent;   // Boolean if objectID is present in the service

  // Constructor
  function __construct($vars) {
    if (isset($vars['uuid']))     { $this->uuid     = $vars['uuid']; }
    if (isset($vars['data_url'])) { $this->data_url = $vars['data_url']; }
    if (isset($vars['config']))   { $this->config   = $vars['config']; }
    if (isset($vars['cycle']))    { $this->cycle    = $vars['cycle']; }
    $this->ids=array();
    $this->total=0;
    $this->columns=array();
    $this->stats='';
  }

  
  function start() {

    $this->postgis_tablename = 'c' . str_replace("-", "_", $this->uuid); 
    echo "Processing job for dataset with uuid: ". $this->uuid ." and tablename: ". $this->postgis_tablename ." and data_url: ". $this->data_url ."\n";

    echo "  --------------------------------------------  \n";
    echo "Connecting to the datastore \n";
    if (!$this->postgis_connect()){
      return array(
        'error' => array(
          'status' => "error",
          'message' => "Error connecting to the datastore"
        )
      );
    }
    echo "Connected to the postgis database. \n";

    
    echo "  --------------------------------------------  \n";
    echo "Starting the data process. \n";
    if (!$this->process()){
      return array(
        'error' => array(
          'status' => "error",
          'message' => "Error connecting to the datastore"
        )
      );
    }
    
    // Create file


    $result = array(
      'uuid' => $this->uuid,
      'geo' => $this->geo,
      'worker_status' => 'completed'
    );

    return $result;
  }


  function process(){

    /**************************************************************/
    // Safe to assume all arcGIS data is geo.
    $this->geo = '1'; 
    
    // get stats
    echo "--- ARCGIS: GET STATS --------------------------------------------------------------------- \n";
    if ($stats = $this->get_stats()) {
      echo "Stats received. \n"; 
      $stats = json_decode($stats);
      $this->stats = $stats;
      $this->version = $stats->currentVersion;
    } else {
      echo "Error fetching stats \n";
      return array(
        'error' => array(
          'status' => "error",
          'message' => "Did not receive any stats."
        )
      );
    }

    // get ids
    echo "--- ARCGIS: GET ID's ---------------------------------------------------------------------- \n";
    if ($id_array = $this->get_ids()) {
      echo "id's received. \n";
      $this->total = count($id_array);
      $id_string = implode(',',$id_array);
      $this->ids = $id_array;
      echo "Received $this->total ID's \n";
    } else {
      return array(
        'error' => array(
          'status' => "error",
          'message' => "Did not receive any ID's."
        )
      );
    }

    // Drop existing table
    echo "Dropping existing table. \n";
    $sql = "DROP TABLE IF EXISTS $this->postgis_tablename;";
    echo "$sql \n";
    $pgResult = $this->postgis_query($sql);

    // Create table
    echo "--- ARCGIS: CREATE TABLE ----------------------------------------------------------------- \n";
    if ($db = $this->create_table()){
      echo "Creating table finished. \n";
    } else {
      return array(
        'error' => array(
          'status' => "error",
          'message' => "Error creating table."
        )
      );
    }

    
    // Start the processing of all objects
    echo "--- ARCGIS: Synchronise ----------------------------------------------------------------- \n";
    if( !$this->sync() ) {
      return array(
        'error' => array(
          'status' => "error",
          'message' => "There was an error during synchronisation."
        )
      ); 
    }
    
    echo "Succesfully finished parsing the feed. \n";

    return TRUE;
  }


  /** 
   * Get stats from a given q->url
   * 
   * @return string 
   *   String with json result or FALSE
   **/
  function get_stats(){
    
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
    
    echo "Fetching all objectIds. \n";

    $params = array(
      "f"               => "pjson",
      "objectIds"       => "",
      "where"           => "1=1",
      "returnIdsOnly"   => "true",
      "text"            => "",
      "returnGeometry"  => "false"
    );

    $ids = $this->get($params,"/query");
    
    if ($ids) {
      $ids = json_decode($ids);
      $ids = (array)$ids->objectIds; 
    }

    return $ids; 
  }


  function create_table(){
    
    echo "Creating a new table for this set. \n";
    
    $stats = $this->stats; 

    $this->columns = [];
    $fields = $stats->fields; 
    if (count($fields)<1){
      echo "No fields found. \n";
      return FALSE;
    }

    foreach($fields as $f){
      // convert esri types to postgis types
      $this->columns['"' . $f->name . '"'] = "TEXT";
      echo "Added column '" . $f->name . "' with type '" . $this->columns['"' . $f->name .'"'] . "'.";

      // Check if objectID is supplied by the service
      if (strtolower($f->name) == 'objectid') {
        $this->objectidPresent = TRUE;
        echo "ObjectID is supplied by the service. \n";
      }
    }

    if(!$this->objectidPresent) {
      echo "ObjectID not supplied by the service, adding it to the columns \n";
      $this->columns['"OBJECTID"'] = "text";
      echo "Added column 'OBJECTID' with type 'text'. \n";
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
    echo "Creating table with name: $this->postgis_tablename and columns: $columnstring \n";
    
    $sql="CREATE TABLE IF NOT EXISTS $this->postgis_tablename (cid serial PRIMARY KEY, $columnstring);"; 
    
    $pgResult = $this->postgis_query($sql); 
    
    if ($pgResult) {
      echo "New table created. \n";
      $this->columnNames = $fields;
      return TRUE;
    } else {
      echo "There was an error creating the table. \n";
      return FALSE; 
    }
  }


  /** 
   * Main loop to process all items
   * 
   **/
  function sync(){
    
    $cycle = $this->cycle;
    $total = count($this->ids);
    $cycles = ceil($total/$cycle);
    echo "Starting the synchronisation of $total elements: $cycles cycles of $cycle elements. \n";

    $counter=0;
    while (count($this->ids)>0){
      $counter++;
      // Create a separate array with the id's to process.
      // id's are substracted from main id array
      $id_list = array_splice($this->ids, 0 , $cycle);

      if (count($id_list)<1){
        echo "No id's present in the list. \n";
        return array(
          'error' => array(
            'status' => "error",
            'message' => "No id's present in the list"
          )
        );
      }
      
      // sometimes objectid 0 is returned. This gives troubles, so removing it.. 
      $id_list = array_diff($id_list, array('0'));

      echo "Fetching records for cycle $counter out of $cycles (Records ". ($counter -1)*$cycle . " to " . (($counter)*($cycle)) . ") \n";
      echo "Number of objects left in queue: " . count($this->ids) . "\n";

      $ids = join($id_list, ",");
      echo "Getting data for " . count($id_list) . "\n"; // object-id's: " . $ids . "\n";

      // Fetch records
      $records = $this->get_records($ids);

      // get_records returns a string, convert to object. 
      $records = json_decode($records);

      if (!$records) {
        echo "No records received for cycle $counter. \n";
        return array(
          'error' => array(
            'status' => "error",
            'message' => "No records received for cycle $counter."
          )
        );
      }

      echo "Received " . count($records->features) . " records for cycle $counter.  Sending the records to the database. \n";
      if (count($records->features) == count($id_list)) {
        $result = $this->process_records($records, $id_list);
        if ($result == FALSE) {
          echo "Error during syncing. Stopping the process. \n";
          return array(
            'error' => array(
              'status' => "error",
              'message' => "Error during syncing. Stopping the process."
            )
          );
        }
      } else {
        echo "The number of received records (" . count($records->features) . ") does not match the number of requested records (" . count($id_list) . ") \n"; 
        return array(
          'error' => array(
            'status' => "error",
            'message' => "The number of received records (" . count($records->features) . ") does not match the number of requested records (" . count($id_list) . ")"
          )
        );
      }
        
      echo "Sync result: $result \n";
    }

    echo "Processed all IDs. \n";

    return TRUE; 
  }

  /**
   * Get record information of id's
   **/
  function get_records($ids){
    echo "Getting record information for arcGIS service version: " . $this->stats->currentVersion . "\n";

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
    } else {
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
    }

    $result = $this->get($params,"/query");

    if ($result) {
      return $result; 
    } else {
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
      echo "Error: Records ($recordCount) and current Ids ($idCount) do not match. \n";
      return FALSE; 
    }

    echo "Starting the process of $recordCount records. \n"; 

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

    $sql = 'INSERT INTO ' . $this->postgis_tablename . ' (' . implode(',',$keys) . ") VALUES " . implode(',',$values) . ";";
    //echo "Constructed SQL: " . $sql ."\n";

    $pgResult = $this->postgis_query($sql);
    if ($pgResult) {
      echo "Saved values to the table. \n";
    } else {
      echo "Error saving values to the table. \n";
      return FALSE;
    }
    
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
      
      echo "Found a multipoint \n";
      echo "Geometry values: ". implode(",",$geometry['points'][0]) ."\n";
      
      $point = $geometry_values['points'][0];
      echo "Point: " . implode(",", $geometry['points'][0]) ."\n";
      
      $wkt = "ST_GeomFromText('POINT(" . implode(' ',$geometry['points'][0]) . ")', 4326)";
      echo "wkt" . $wkt ."\n";

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
      echo "Found a geometry field, but unable to process. \n";
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

    //echo "url: ". $this->data_url . $q . "\n";
    $result = @file_get_contents($this->data_url . $q);
    
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


  /**
   * Open a PostGIS Connection
   * @return Connection resource or FALSE
   **/
  function postgis_connect() {

    $conf = $this->config;
    $this->postgis_conn = pg_connect('host='.$conf['postgis']['host'] . ' port=' . $conf['postgis']['port'] . ' dbname=' . $conf['postgis']['database'] . ' user=' . $conf['postgis']['username'] . ' password=' . $conf['postgis']['password'] .  ' connect_timeout=5');

    return $this->postgis_conn; 
  }

  /**
   * Close an open PostGIS connection
   **/
  function postgis_close($c) {
    return pg_close($c);
  }

  /**
   * Execute a postGIS query command
   **/
  function postgis_query($sql){

    $output = FALSE; 
    // open a connection
    $conn = $this->postgis_connect();

    if (!$conn) {
      echo "Error connecting to postGIS. \n";
    } else {
      $result = pg_query($conn, $sql);
      if(!$result){
        echo "Error executing query. [details: " . pg_last_error(). "] \n";
      }
      pg_close($conn); 
      $output = $result;
    }

    return $output;
  }

  function convert_esri_types(){
    // Convert esri response fields to postgis table datatypes
    $esriconvertable = array(
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
      "Latitude"      => "TEXT"
    );
  }
}

?>