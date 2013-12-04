<? 

/** 
 * CSV Processor Class
 *
 **/
class csv {

  public $job_errorlog;  // job processing error log
  public $geo;           // is the set geodata? 
  public $worker_error;   // Text with error to send to API
  public $worker_status;  // Status of the worker

  private $job;           // incoming job item
  private $processed_job; // processed job data
  
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
   * @return $processed_job Array with processed job properties
   * 
   **/
  function __construct($job){

    $this->job_errorlog = array(); 

    message("CSV: Starting the process of a new CSV file.");
    
    $this->connection = drupal_connect();
    if (!$this->connection){
      message("CSV: Error connecting to the job queue database. ");
    } else {
      message("CSV: Connected to the job queue database. ");
      $this->job = $job; 
      $this->tablename = "c".str_replace("-", "_", $job['uuid']); 
      $this->id = $job['ID'];
      $uuid = $job['uuid'];

      message("CSV: Fetching data for uuid: " . $job['uuid']);
      message("CSV: Fetching data for queue id: " . $job['ID']);
      message("CSV: Fetching data for tablename: " . $this->tablename);
      
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
        $this->job_errorlog[] = date('c').';Error Fetching data from CMS: ' . mysql_error();
        message("Error Fetching data from CMS: " . mysql_error());
      } else {
        $row = mysql_fetch_assoc($result);
        $this->datafile_uri = $row["uri"]; 
        
        // change drupal's public:// to http sitename
        // how to get public directory? 
        $this->datafile_uri = str_replace('public://', '/home/columby/www/columby.dev/sites/default/files/', $this->datafile_uri);
        message("CSV: Starting the process of node with uuid $this->tablename and datafile. ");
        
        if(empty($this->datafile_uri)){
          $this->job_errorlog[] = date('c').";The reference to the file that should be processed is empty. "; 
          message("The reference to the file that should be processed is empty. ");
        } else {
          // start processing the csv-file
          $this->process();
        }
      }

      // update queue item
      if (count($this->processed_job['job_errorlog']) > 0) {
        $e = implode(', ', $this->processed_job['job_errorlog']);
        $this->update_queue('error', $e);
        $this->update_queue('done', "1");
        $this->update_queue('processing', "0");
      }

      // Return job processing info
      $this->processed_job = array();
      $this->processed_job['job'] = $this->job; 
      $this->processed_job['job_errorlog'] = $this->job_errorlog;

    }

    return TRUE;
  }


  /**
   * Process the csv-file
   *
   **/
  function process(){
    
    message("Opening file for data processsing");
    
    $max_line_length = 10000;
    
    // Open the file for reading
    if (($handle = fopen("$this->datafile_uri", 'r')) !== FALSE) {
      message("File opened");

      $file = new SplFileObject($this->datafile_uri);
      
      $delimiter = $file->getCsvControl();
      message("Processing CSV with delimiter: ");
      
      $columns = fgetcsv($handle, $max_line_length, $delimiter[0]);
      message("processed first line.");;
      $columns = $this->sanitize_columns($columns); 
      
      $column_types = array();

      // check for WKT values
      // in_array, strtolower
      for ($k=0; $k<count($columns); $k++) {
        if ( ($columns[ $k] == 'WKT') || ($columns[ $k] == 'wkt') ){
          $this->wkt_column = $k;
          $this->geo = TRUE;
          message("Found a wkt geometry column: $this->wkt_column.");
        }
      }
      
      // remove wkt from sql insert statement (processed as the_geom)
      unset ($columns[ $this->wkt_column]);

      $s = join(" text,", $columns); 
      $s .= " text";

      $sql="CREATE TABLE $this->tablename (cid serial PRIMARY KEY, $s, the_geom geometry, date_created timestamp, date_updated timestamp);";      
      message("Creating table: $sql");

      $pgResult = postgis_query($sql);
      
      if ($pgResult) {
        message("Table created.");
      
        // Create a sql statement to work with
        $sql_pre = '';
        $sqls = []; 

        // Counter for rows (and is cid)
        $i = 1;

        message("Starting data processing. ");

        // Only process when there is no error
        while( (($datarow = fgetcsv($handle, $max_line_length, ',')) !== FALSE) && (!$this->worker_error)) {
            
          // Create the right sql statement based on data in the received row
          $sql = $this->prepare_sql_statement($i, $datarow); 
          // Add the single SQL-lines to an array
          $sqls[] = $sql;
          // Create an sql statement for 500 rows. 
          if(count($sqls) > 500){
            message("Processed 500 lines, sending data. ");
            $this->send($sqls);
            $sqls = []; 

            $this->update_queue("processed", $i);
          }

          $i++;
        }
        
        if (!$this->worker_error) {
          message("Sending final " . count($sqls) . " lines. ");
          $this->send($sqls);
          $this->update_queue("processed", $i);
        }

        message("Finished data processing. ");
        $this->update_queue("processing","0");
        $this->update_queue("done","1");

      } else {
        $this->job_errorlog[] = date('c').";No connection to postgis!";
      }
    } else {
      $this->job_errorlog[] = print_r(error_get_last());
    }
  }
  
  /**
   * Send data to the database
   * 
   * @param $sqls array of sql queries
   * 
   **/
  function send($sqls) {

    message("Sending data to: $this->tablename", "nolog"); 
    $sql_pre = join($sqls,", ");
    $sql = "INSERT INTO $this->tablename VALUES $sql_pre;";
    $sql_pre = "";
    
    // send the query
    $pgResult = postgis_query($sql);
    message("Send result: $pgResult",'nolog'); 
    // Check results
    if ($pgResult == 'success') {
      message('Send success!','nolog'); 
    } else {
      $this->update_queue('error', 'error executing query.');
      $this->job_errorlog[] = "Error executing query: $pgResult";
      message("Error executing query: $pgResult");
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
    message("Setting queue item: $sql","nolog"); 
    $this->job[$col] = $val; 
    $q = mysql_query($sql, $conn); 
    if (!$q) {
      message("Error: ".mysql_error(),'nolog'); 
    } else {
      message("Item written to queue-table ($col: $val)",'nolog');  
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