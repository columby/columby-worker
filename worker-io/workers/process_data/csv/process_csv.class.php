<?php 

class ProcessCSV {

  // Class variables
  var $uuid;
  var $config;
  var $postgis_tablename;     // name of postgis database table
  var $postgis_conn;
  var $geo;

  // Constructor
  function __construct($uuid, $config) {
    $this->uuid = $uuid;
    $this->config = $config;
  }

  function start() {
    $this->postgis_tablename = 'c' . str_replace("-", "_", $this->uuid); 
    echo "Processing job for dataset with uuid: ". $this->uuid ."and tablename: ". $this->postgis_tablename ."\n";

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
    if (!$this->process_data()){
      return array(
        'error' => array(
          'status' => "error",
          'message' => "Error connecting to the datastore"
        )
      );
    }
    
    // Create File

    
    $result = array(
      'uuid' => $this->uuid,
      'geo' => $this->geo,
      'worker_status' => 'completed'
    );

    return $result; 
  }


  function process_data() {
    $conf = $this->config;
    $file_uri = $conf['columby']['host'] . '/download/'. $this->uuid;
    // open file
    echo "Opening file for data processsing: $file_uri \n";
    $max_line_length = 10000;
    $handle = fopen($file_uri,'r');
    if ($handle === FALSE) {
      echo print_r(error_get_last());
      return FALSE;
    }
    
    // Check csv settings
    echo "Checking CSV settings \n";
    $file = new SplFileObject($file_uri);
    $delimiter = $file->getCsvControl();
    echo "Processing CSV with delimiter: " . $delimiter[0] . "\n";

    // Process column
    $columns = fgetcsv($handle, $max_line_length, $delimiter[0]);
    
    // check for WKT values, we need the location for later data processing
    if ($wkt_column = array_search(strtolower('wkt'),array_map('strtolower',$columns))) {
      $this->geo = 1;
      echo "Found a wkt geometry column: $wkt_column. \n";
      // Remove wkt from sql insert statement (processed as the_geom)
      unset ($columns[ $wkt_column]);
    }

    $columns = $this->sanitize_columns($columns);
    echo "Processed first line with column data: " . implode($columns,',') . "\n";

    // Create column sql string (all text columns for now)
    $s = join(" text,", $columns); 
    $s .= " text";

    // Drop existing table
    echo "Dropping existing table. \n";
    $sql = "DROP TABLE IF EXISTS $this->postgis_tablename;";
    echo "$sql \n";
    $pgResult = $this->postgis_query($sql);
    //echo "Result: $pgResult. \n";

    // Create table
    $sql="CREATE TABLE $this->postgis_tablename (cid serial PRIMARY KEY, $s, the_geom geometry, createdAt timestamp, updatedAt timestamp);";      
    echo "Preparing table: $sql \n";
    $pgResult = $this->postgis_query($sql);
    //echo "Result: $pgResult. \n";  

    // Process the data
    if (!$pgResult) {
      echo "Error creating table. \n";
      return FALSE;
    }
    echo "Table created. \n";
    
    $sql_pre = '';
    $sqls = array(); 
    // Counter for rows (and is cid)
    $i = 1;
    echo "Starting data processing. \n";

    // Only process when there is no error
    while( ($datarow = fgetcsv($handle, $max_line_length, ',')) !== FALSE) {
        
      // Create the right sql statement based on data in the received row
      // add cid (columby unique id)
      $sql = "(". $i . ",";
      //save wkt value for later processing
      if ($wkt_column) {
        $wkt = $datarow[ $wkt_column]; 
        unset($datarow[ $wkt_column]); 
      }
      // put all columns except wkt in the sql
      foreach ($datarow as $field) { 
        $sql .= "'" . pg_escape_string($field) . "',"; 
      }
      // add the_geom from WKT
      if (isset($wkt)) {
        $sql .= " ST_GeomFromText('" . pg_escape_string($wkt) . "', -1),"; 
      } else {
        $sql .= " NULL,";
      }
      // Add dates
      $sql .=  "'".date('Y-m-d H:i:s') . "','" . date('Y-m-d H:i:s') . "') ";
      
      // Add the single SQL-lines to an array
      $sqls[] = $sql;
      
      // Create an sql statement for 500 rows. 
      if(count($sqls) > 500){
        echo "Processed 500 lines, sending data. \n";
        echo "Sending data to: $this->postgis_tablename. \n";
        $sql_pre = join($sqls,", ");
        $sql = "INSERT INTO $this->postgis_tablename VALUES $sql_pre;";
        $sql_pre = "";

        // send the query
        $pgResult = $this->postgis_query($sql);
        // Check results
        if ($pgResult != FALSE) {
          echo "Send success! \n";
        } else {
          echo "Error executing query: $pgResult \n";
          return FALSE;
        }

        $sqls = array(); 
        
      }

      $i++;
    }

    // send final lines 
    if(count($sqls) > 0){
      echo "Sending final " . count($sqls) . " lines \n";
      $sql_pre = join($sqls,", ");
      $sql = "INSERT INTO $this->postgis_tablename VALUES $sql_pre;";
      $sql_pre = "";

      // send the query
      $pgResult = $this->postgis_query($sql);
      // Check results
      if ($pgResult != FALSE) {
        echo "Send success! \n";
      } else {
        echo "Error executing query: $pgResult \n";
        return FALSE;
      }

      $sqls = array(); 
    }

    echo "Finished data processing. \n";
    
    return TRUE;
  }



  /**
   * Open a PostGIS Connection
   * 
   * @return Connection resource or FALSE
   *
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


  /** 
   * Sanitize columns for postGIS database
   * 
   **/
  function sanitize_columns($columns) {
    $fields = array(); 
    foreach ($columns as $field) {
      $field = str_replace(' ', '_', $field);
      $field = str_replace('.', '_', $field);
      $field = strtolower($field);
      $field = '"' . $field . '"';
      $fields[]=$field;
    }
    return $fields;
  }
}

?>