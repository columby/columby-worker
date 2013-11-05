<? 

/** 
 * CSV Processor Class
 **/
class csv {

  private $q;           // incoming question
  private $connection;  // connection to drupal database
  private $id;          // id of queue item
  private $uuid;        // uuid of dataset node
  private $tablename;   // 
  private $datafile_uri;    // uri to storage of datafile
  private $wkt_column;  // reference to the column number with the wkt_values

  function __construct($_q){

    message("CSV: Starting a new csv item."); 

    $this->connection = drupal_connect();
    message("CSV: Connected to database: $this->connection");
    $this->q = $_q; 
    $this->uuid = $_q->array['uuid']; 
    $uuid = $this->uuid;
    message("CSV: Fetching data for uuid: $this->uuid");
    $this->id = $_q->array['ID']; 
    message("CSV: Fetching data for queue id: $this->id");
    $this->tablename = "c".str_replace("-", "_", $this->uuid); 
    message("CSV: Fetching data for tablename: $this->tablename");
    
    $sql = "SELECT 
      n.uuid,
      n.nid,
      field_data_field_file.`field_file_fid`,
      file_managed.uri
      FROM node n
      LEFT JOIN field_data_field_file ON n.nid = field_data_field_file.`entity_id`
      LEFT JOIN file_managed ON field_data_field_file.`field_file_fid` = file_managed.`fid`
      WHERE n.uuid = '$uuid'";
    $result = mysql_query($sql,$this->connection);
    
    while ($row = mysql_fetch_assoc($result)) {
      //message("******* *******");
      message(print_r($row));
      $this->datafile = $row["uri"]; 
      // change drupal's public:// to http sitename
      $this->datafile = str_replace('public://', '/home/columby/www/live/sites/default/files/', $this->datafile);
      message("CSV: Starting the process of node with uuid $this->tablename and datafile: $this->datafile");
    }
    
    if(empty($this->datafile)){
      $this->dbq_put('error', 'The file that should be processed is empty or doesn\'t exist'); 
      message("CSV: The file that should be processed is empty or doesn't exist. ($this->uuid)");
    } else {
      message("CSV: Starting the process of file $this->datafile. ($this->uuid)");
      // start processing the csv-file
      $this->dbq_go();
    }
  }


  /**
   * 
   * Process the csv-file
   *
   **/
  function dbq_go(){
    
    message("Starting the process of file $this->datafile. ($this->uuid)");
    $max_line_length = 10000;
    
    // Open the file for reading
    if (($handle = fopen("$this->datafile", 'r')) !== FALSE) {
      message("File opened ($this->datafile)");

      // message("create table $tablename"). 
      // If table already existed then it was removed in the render() function
      
      $file = new SplFileObject($this->datafile);
      $delimiter = $file->getCsvControl();
      message("Processing CSV with delimiter: " . $delimiter[0]);
      $columns = fgetcsv($handle, $max_line_length, $delimiter[0]);
      message("processed first line: ");
      message(print_r($columns));
      $columns = $this->sanitize_columns($columns); 
      
      $column_types = array();

      // check for WKT values
      for ($k=0; $k<count($columns); $k++) {
        if ( ($columns[ $k] == 'WKT') || ($columns[ $k] == 'wkt') ){
          $this->wkt_column = $k; 
          message("Found a wkt geometry column: $this->wkt_column.");
        }
      }
      
      // remove wkt from sql insert statement
      unset ($columns[ $this->wkt_column]); 

      $s = join(" text,", $columns); 
      message("sql: " . $s);

      $sql="CREATE TABLE $this->tablename (cid serial PRIMARY KEY, $s text, the_geom geometry, date_created timestamp, date_updated timestamp);";      
      message("Creating table: $sql"); 

      $pgResult = pgq($sql);
      
      if ($pgResult) {
        message("Created table $sql, $pgResult");
      
        // Create a sql statement to work with
        $sql_pre = '';
        // Counter for rows
        $i = 1;

        // get the data from the csv, read each line. 
        // Process the file in chunks of 500 lines
        message("Starting data processing.");
        while(($datarow = fgetcsv($handle, $max_line_length, ',')) !== FALSE) {
          
          $sql_pre = "";
          // add cid (columby unique id)
          $sql_pre .= "(". $i . ",";
          //save wkt value for later processing
          if ($this->wkt_column) {
            $wkt = $datarow[ $this->wkt_column]; 
            unset($datarow[ $this->wkt_column]); 
            //echo ('*** found wkt: $wkt'); 
          } else {
            //echo ('*** NOT found wkt.');  
          }
          

          // put all columns except wkt in the sql
          foreach ($datarow as $field) {
            $sql_pre .= "'" . pg_escape_string($field) . "',"; 
          }

          // add the_geom from WKT
          if (isset($wkt)) {
            //echo(' **** Setting geom from tekst: $wkt' . pg_escape_string($wkt)); 
            $sql_pre .= " ST_GeomFromText('" . pg_escape_string($wkt) . "', -1),"; 
          } else {
            $sql_pre .= " NULL,";
          }
          // Add dates
          $sql_pre .=  "'".date('Y-m-d H:i:s') . "','" . date('Y-m-d H:i:s') . "') "; 
          
          //echo (date('c') . " - dbq_go: data: $sql_pre \n");
          // Add the single SQL-lines to an array
          $sqls[] = $sql_pre;
        
          // Create an sql statement for 500 rows. 
          if(count($sqls) > 500){
            message("Processed 500 lines.");

            $this->dbq_send($sqls);   // error checking? 
            // reset
            $sqls = []; 

            message("Processed lines.");
            $this->q->put("processed", $i);
          }

          $i++;
        }
      
        message("Processed final lines.");
        
        $processed = $this->dbq_send($sqls);  // error checking? 
        // reset
        $sqls = []; 
      
        if ($processed) {
          message("Finished processing final lines.");
          $this->dbq_put("processing","0");
          $this->dbq_put("done","1");
        } else { 
          message("Error finishing processing final lines!");
        }
      } else {
        message("No connection to postgis!");

      }
    } else {
      $e=print_r(error_get_last());
      message(print_r($e)); 
      message("An error occured reading the file.");
      $this->dbq_put('error', print_r($e));
      $this->dbq_put('done', "1");
      $this->dbq_put('processing', "0");
    }
  }
  
  function dbq_send($sqls) {

    echo (date('c') . " - dbq_send - Sending data to: $this->tablename \n"); 
    $sql_pre = join($sqls,", ");
    //echo (date('c') . " - dbq_send - sqldata: $sql_pre \n"); 
    $sql = "INSERT INTO $this->tablename VALUES $sql_pre;";
    $sql_pre = "";
    //echo (date('c') . " - dbq_send - Sending data: $sql \n"); 

    // send the query
    // pgq returned false of result; 
    $pgResult = pgq($sql);
    echo (date('c') . " - dbq_send - Send result: $pgResult \n"); 

    // Check results
    if ($pgResult) {
      echo (date('c') . " - dbq_send - Send success! \n"); 
      //$this->q->put("split",$i);
      //$this->q->put("total",$i); // is dit nodig? 
      //$this->q->put("processed",100); 
    } else {
      echo (date('c') . " - dbq_send - Error sending the query: $sql\n"); 
      $error = pg_last_error(); 
      $escaped = addslashes($error); 
      echo("#### escaped: " . $escaped);
      $this->dbq_put('error', $escaped);
    }

    return $pgResult; 
  }

  function dbq_put($col,$val){

    $conn = $this->connection; 
    $id = $this->id; 
    $val = addslashes($val); 
    $sql = "UPDATE columby_queue SET $col='$val' WHERE ID=$id"; 
    echo (date('c') . " - setting queue item: $sql \n"); 
    $q = mysql_query($sql, $conn); 
    $this->q->array[$col] = $val; 
    if (!$q) {
      echo (date('c') . " - error: ".mysql_error()."\n"); 
    } else {
      echo (date('c') . " - item written to queue-table ($col: $val) \n");  
    }
  }

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