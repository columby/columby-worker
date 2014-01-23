<? 

/** 
 * CSV Processor Class
 *
 **/
class csv {

  public $geo;           // is the set geodata? 
  public $worker_error;   // Text with error to send to API
  public $worker_status;  // Status of the worker

  private $local_file_path;
  private $job;           // incoming job item
  
  private $connection;    // connection to drupal database
  private $tablename;     // name of postgis database table
  private $datafile_uri;  // uri to storage of datafile
  private $wkt_column;    // reference to the column number with the wkt_values
  private $id;            // reference of current job id

  /** 
   * Construct the CSV Processor
   *
   * @param $job Array with job properties
   *
   * @return TRUE
   * 
   **/
  function __construct($job){
    
    $local_file_path = '/home/columby/www/columby.dev/sites/default/files/'; 

    $l = get_log(); 
    message('log:','log','DEBUG');
    message(implode(",",$l),'log','DEBUG');
    
    message("CSV: Starting the process of a new CSV file.",'log','NOTICE');
    
    $this->connection = drupal_connect();
    if (!$this->connection){
      message("CSV: Error connecting to the job queue database. ",'log','ERROR');
    } else {
      message("CSV: Connected to the job queue database. ",'log','NOTICE');
      $this->job = $job; 
      $this->tablename = "c".str_replace("-", "_", $job['uuid']); 
      $this->id = $job['ID'];
      $uuid = $job['uuid'];

      message("CSV: Fetching data for uuid: " . $job['uuid'],'log','NOTICE');
      message("CSV: Fetching data for queue id: " . $job['ID'],'log','NOTICE');
      message("CSV: Fetching data for tablename: " . $this->tablename,'log','NOTICE');
      
      // Fetch required data from CMS
      $sql = "SELECT 
        n.uuid,
        n.nid,
        field_data_field_file.`field_file_fid`,
        file_managed.uri
        FROM node n
        LEFT JOIN field_data_field_file ON n.nid = field_data_field_file.`entity_id`
        LEFT JOIN file_managed ON field_data_field_file.`field_file_fid` = file_managed.`fid`
        WHERE n.uuid = '$uuid'
        LIMIT 1;";

      $result = mysql_query($sql, $this->connection);
      
      // Process results
      if (!$result) {
        message("Error Fetching data from CMS: " . mysql_error(),'log','ERROR');
      } else {
        $row = mysql_fetch_assoc($result);
        $this->datafile_uri = $row["uri"]; 
        
        // change drupal's public:// to http sitename
        // how to get public directory? 
        $this->datafile_uri = str_replace('public://', $local_file_path, $this->datafile_uri);
        message("CSV: Starting the process of node with uuid $this->tablename and datafile. ",'log','NOTICE');
        
        if(empty($this->datafile_uri)){
          message("The reference to the file that should be processed is empty. ",'log','ERROR');
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
        $this->update_queue('done', "1");
        $this->update_queue('processing', "0");
      }
    }

    return TRUE;
  }


  /**
   * Process the csv-file
   *
   **/
  function process(){
    
    message("Opening file for data processsing",'log','NOTICE');
    
    $max_line_length = 10000;
    
    // Open the file for reading
    if (($handle = fopen("$this->datafile_uri", 'r')) !== FALSE) {
      message("File opened",'log','NOTICE');

      $file = new SplFileObject($this->datafile_uri);
      
      $delimiter = $file->getCsvControl();
      message("Processing CSV with delimiter: ",'log','NOTICE');
      
      $columns = fgetcsv($handle, $max_line_length, $delimiter[0]);
      message("processed first line.",'log','NOTICE');
      $columns = $this->sanitize_columns($columns); 
      
      $column_types = array();

      // check for WKT values
      // in_array, strtolower
      for ($k=0; $k<count($columns); $k++) {
        if ( ($columns[ $k] == 'WKT') || ($columns[ $k] == 'wkt') ){
          $this->wkt_column = $k;
          $this->geo = TRUE;
          message("Found a wkt geometry column: $this->wkt_column.",'log','NOTICE');
        }
      }
      
      // remove wkt from sql insert statement (processed as the_geom)
      unset ($columns[ $this->wkt_column]);

      $s = join(" text,", $columns); 
      $s .= " text";

      $sql="CREATE TABLE $this->tablename (cid serial PRIMARY KEY, $s, the_geom geometry, createdAt timestamp, updatedAt timestamp);";      
      message("Creating table: $sql",'log','NOTICE');

      $pgResult = postgis_query($sql);
      
      if ($pgResult) {
        message("Table created.",'log','NOTICE');
      
        // Create a sql statement to work with
        $sql_pre = '';
        $sqls = []; 

        // Counter for rows (and is cid)
        $i = 1;

        message("Starting data processing. ",'log','NOTICE');

        // Only process when there is no error
        while( (($datarow = fgetcsv($handle, $max_line_length, ',')) !== FALSE) && (!$this->worker_error)) {
            
          // Create the right sql statement based on data in the received row
          $sql = $this->prepare_sql_statement($i, $datarow); 
          // Add the single SQL-lines to an array
          $sqls[] = $sql;
          // Create an sql statement for 500 rows. 
          if(count($sqls) > 500){
            message("Processed 500 lines, sending data. ",'log','NOTICE');
            $this->send($sqls);
            $sqls = []; 

            $this->update_queue("processed", $i);
          }

          $i++;
        }
        
        // Finish the job

        if (!$this->worker_error) {
          message("Sending final " . count($sqls) . " lines. ",'log','NOTICE');
          $this->send($sqls);
          $this->update_queue("processed", $i);
        }

        message("Finished data processing. ",'log','NOTICE');
        $this->update_queue("processing","0");
        $this->update_queue("done","1");

      } else {
        message("No connection to postgis!",'log','ERROR');
      }
    } else {
      message(print_r(error_get_last()), 'log','ERROR');
    }
  }
  
  /**
   * Send data to the database
   * 
   * @param $sqls array of sql queries
   * 
   **/
  function send($sqls) {

    message("Sending data to: $this->tablename",'log','DEBUG'); 
    $sql_pre = join($sqls,", ");
    $sql = "INSERT INTO $this->tablename VALUES $sql_pre;";
    $sql_pre = "";
    
    // send the query
    $pgResult = postgis_query($sql);
    message("Send result: $pgResult",'log','DEBUG');
    // Check results
    if ($pgResult != FALSE) {
      message('Send success!','log','DEBUG');
    } else {
      message("Error executing query: $pgResult",'log','ERROR');
    }

    return $pgResult; 
  }

  /**
   * Update the queue item
   * 
   * @param $col 
   * @param $val
   * 
   **/
  function update_queue($col, $val) {

    $conn = $this->connection; 
    $id = $this->id; 
    $val = addslashes($val); 
    $sql = "UPDATE columby_queue SET $col='$val' WHERE ID=$id"; 
    message("Setting queue item: $sql",'log','DEBUG');
    $this->job[$col] = $val;  
    $q = mysql_query($sql, $conn); 
    if (!$q) {
      message("Error: ".mysql_error(),'log','NOTICE');
    } else {
      message("Item written to queue-table ($col: $val)",'log','DEBUG');
    }
  }


  /************* HELPER FUNCTIONS *****************/

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

  /** 
   * Prepare the sql row statement
   * 
   * @param
   *
   * @return
   *
   **/
  function prepare_sql_statement($cid, $row) {
    $sql = ''; 

    $sql = "";
    // add cid (columby unique id)
    $sql .= "(". $cid . ",";

    //save wkt value for later processing
    if ($this->wkt_column) {
      $wkt = $row[ $this->wkt_column]; 
      unset($row[ $this->wkt_column]); 
    }

    // put all columns except wkt in the sql
    foreach ($row as $field) { $sql .= "'" . pg_escape_string($field) . "',"; }

    // add the_geom from WKT
    if (isset($wkt)) {
      $sql .= " ST_GeomFromText('" . pg_escape_string($wkt) . "', -1),"; 
    } else {
      $sql .= " NULL,";
    }

    // Add dates
    $sql .=  "'".date('Y-m-d H:i:s') . "','" . date('Y-m-d H:i:s') . "') "; 
          
    return $sql; 
  }
}