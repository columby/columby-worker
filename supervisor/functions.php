<?php

/** 
 *
 * Background data processor
 * 
 **/

error_reporting(E_ALL);

require_once("config/settings.php");
require_once("parsers/queue.php");
require_once("drupal/drupalREST.php");
require_once("logger/logger.php");

// Set vars
$processing = FALSE;     // Is there a job running?
$job = array();          // Array with job information
$logger; 
$error = FALSE;
$errors = array();


/** 
 * Main function for processing the job-item
 *
 * * Create job item
 * * Drop existing table
 * * Reset the queue item
 * * Determine job type
 * * Parse job
 * * Save parse-log (in columby_worker_log)
 * * Save queue error-log (in columby_queue)
 * * Update Drupal node? 
 *
 **/
function process_job(){

  global $processing, $job, $logger, $errors, $error;

  $logger = new Logger(); 

  // Start with clean job-object
  $job = [];
  
  // Set processing var to true, since processing can take longer than the check_queue interval
  $processing = TRUE;
  $logger->add("---------------------------------- --------------------- ---------------------------------- ");
  $logger->add("---------------------------------- START PROCESSING JOB  ---------------------------------- ");
  $logger->add("---------------------------------- --------------------- ---------------------------------- ");
  $logger->add("--- Startdate: " . date('c'));

  // Initialize api
  $logger->add("--- CONNECTING TO THE COLUMBY API --------------------------------------------------------- ");
  global $drupaluser;
  $api = new DrupalREST($drupaluser['endpoint'], $drupaluser['username'], $drupaluser['pass']);
  // get the required csrf token
  $api_token = $api->request_token();
  $logger->add("Columby API: token received.");
  // connect to the api
  $api_connection = $api->connect(); 
  // log in if user is not connected
  $uid = $api_connection['uid'];
  
  if ($uid === 0) {
    $logger->add("Columby API: No active session, logging in...");
    $api_login = $api->login();
    $uid = $api_login['uid'];
  }
  
  $logger->add("Columby API: Logged in with uid: " . $uid);
  
  if ($uid === 0){
    // not connected to the api, stop the process. 
    $logger->add("Columby API: Error connecting to the Columby API");
    $error = TRUE;
    $errors[] = ' Error connecting to the Columby API';
  }

  //** CREATE JOB ITEM **//
  $logger->add("--- RETRIEVING JOB INFORMATION ------------------------------------------------------------ ");
  $retrieved_job = new Job();
  $job = $retrieved_job->job_data; 
  // uuid of the dataset node
  $uuid = $job['uuid'];
  $tablename = "c".str_replace("-", "_", $uuid);
  $logger->add("Updating the workers status to: In progress");
  $fields = array(
    'worker_status'=>'in progress'
  );
  $r = $api->update($uuid, $fields); 

  $logger->add("Sent 'in progress' worker status to the ColumbyAPI. ");
  

  //** DROP EXISTING TABLE **//
  $logger->add("--- REMOVING EXISTING DATA ----------------------------------------------------------------- ");
  $sql = "DROP TABLE IF EXISTS $tablename";
  $result = postgis_query($sql);
  if (!$result) {
    $logger->add("Error connecting to the database. Not processing the data."); 
    $error = TRUE; 
    $errors[] = "Error when trying to remove existing database table. ";
  } 

  // Clear existing database
  if ($error === FALSE) {
    $logger->add("Cleared existing database table.");
    $logger->add("Resetting job parsing status"); 
    $retrieved_job->put("data","");
    $retrieved_job->put("stats","");
    $retrieved_job->put("split","0");
    $retrieved_job->put("total","");
  }

  // Determine the job-type and start processing (separate classes)
  if ($error === FALSE) {
    $logger->add("--- STARTING PARSER --------------------------------------------------------- ");
    $type = $job['type']; 
    $logger->add('Determining job type. '); 
    switch ($type) {
      case "0": //'csv':
        require_once("parsers/csv.php");
        $logger->add("Job type: CSV.");
        $logger->add("Initiating the CSV-parser.");
        $parsed = new csv($job);
      break;
      case "1": //'arcgis10':
        require_once("parsers/arcgis.php");
        $logger->add("Job type: arcgis.");
        $logger->add("Initiating the arcgis-parser.");
        $parsed = new scraper($job);
      break;
      case "2": //'iati':
        require_once("parsers/iati.php");
        $logger->add("Job type: iati.");
        $logger->add("Initiating the iati-parser.");
        $parsed = new iati($job);
      break;
      case null: 
      $parsed = '';
        $logger->add("No job-type found, skipping this task. ");
      break; 
    }
  }
  
  // Export table to csv-file if necessary
  if ($error === FALSE) {
    if (($type == "0")||($type == "1")){
      $logger->add("Exporting table to file. ");
      $file = postgis_export_table($uuid);
      if ($file) {
        $logger->add("Export succeeded.");
        $fields = array(
          'file' => '/home/columby/exports/'.$uuid.'.csv'
        );
        
        if ($api->update($uuid, $fields)){
          $logger->add("Updated the file to " . $fields['file']);
        }
      
      } else {
        $logger->add("CSV Export error.");
        $error=TRUE;
        $errors[] = "CSV Export error.";
      }
    
    } else {
      $logger->add("The dataset is not saved in the columby datastore. File-export is not needed.");
    }
  }


  $fields = array(
    'worker_status' => 'Finished', 
    'sync_date' => date('Y-m-d H:i:s', strtotime('now'))
  );
  if ($parsed->geo){
    $fields['geo'] = (int)$parsed->geo;
    $logger->add("Setting geo to: " . $parsed->geo);
  }

  // Check for errors
  if (count($errors) > 0) {
    $errorList = implode("\n", $errors);
    $logger->add("--- Error report: "); 
    $logger->add($errorList);
    $fields['worker_status'] = 'error';
    $fields['worker_error'] = $errorList;
  } else {
    $logger->add("No errors found during processing.");
  }

  // Finalize worker status
  $logger->add("Setting worker-status to: " . $fields['worker_status']);
  $logger->add("Setting sync date to: " . $fields['sync_date']);
  
  // Send all fields to the API
  $r = $api->update($uuid, $fields);
  $logger->add("Worker status updated.");
  
  // Save the log file. 
  worker_log_save($job['uuid']);

  $logger->add("---------------------------------- ------------------------ ---------------------------------- ");
  $logger->add("---------------------------------- Finished processing data ---------------------------------- ");
  $logger->add("---------------------------------- ------------------------ ---------------------------------- ");

  $processing = FALSE;
}


/**
 * Check the queue for items to be processed. 
 * 
 * @return Boolean
 **/
function check_queue(){
  // Only check when the processor is not running
  global $processing; 
  if ($processing) {
    echo "There is already a job process running. \n";
    return FALSE; 
  }

  // connect to the Drupal database (returns FALSE or connection-resource)
  $conn = drupal_connect();

  // query when there is a connection
  if (!$conn) {
    echo "Error connecting to the CMS database." . "\n";
  
  } else {
    // To be sure, set the processing of items to off.
    $q = mysql_query("UPDATE columby_queue SET processing=0 WHERE processing=1", $conn);

    // check to see if there is (more than) 1 item in the queue for processing
    $q = mysql_query("SELECT COUNT(ID) FROM columby_queue WHERE done=0 AND processing=0", $conn);
    $count = mysql_result($q, 0);

    echo date('c') . " - Job count: " . $count . "\n";
    if ($count > 0) { 
      return TRUE;
    }
  }
  // error in connection, or no items to process
  return FALSE;
}


/**
 * Process messages sent from the parsers
 * 
 * @param $message 
 *   The message text
 * @param $service
 *   service for the message
 * @param $severity
 *   Severity of the message
 *     ERROR: Error conditions.
 *     NOTICE: (default) Normal but significant conditions.
 *     DEBUG: Debug-level messages.
 */
function message($message, $service='log', $severity='INFO'){

  switch ($service){
    
    case "log":
      
      if ($severity=='DEBUG') {
        echo date('c')." - [...] ".$message."\n";
      } else {
        global $logger;
        $logger->add($message, $severity);
      }
      break;
    
    // Send a message to twitter
    case "tweet":
      echo "TWEET: $message \n";
      break;
    
    // Send the message to Drupal Watchdog.
    case "drupal":
      warn($message);
      break;
  }
}


/**
 * Save the worker log object to the database
 *
 * @param $uuid String
 *
 **/
function worker_log_save($uuid){
  // get the job log
  global $job;
  global $logger;

  $logger->add("Saving log for: " . $uuid);

  // only save the log for the right uuid
  if ($job['uuid'] == $uuid) {

    // check if there are log items present
    if (count($logger->log) > 0) {
      
      // connect to the database
      $conn = drupal_connect();
      // convert array to string
      $log = $logger->log;
      $log = implode("\n", $log);
      // escape unwanted characters
      $log = mysql_real_escape_string($log);
      
      // send to database
      $sql = "INSERT INTO columby_worker_log (uuid, log) VALUES ('$uuid', '$log')";
      
      $result = mysql_query($sql,$conn) or die(mysql_error());
      
    } else {
      echo "There are no items in the log, not sent to database. "; 
    }
  } else {
    echo "$uuid is not the current log uuid. "; 
  }
}




/************* DATABASE FUNCTIONS *************/

/**
 * Open a PostGIS Connection
 * 
 * @return Connection resource or FALSE
 *
 **/
function postgis_connect() {

  global $columby_pg_data;

  $conn_string = 'host='.$columby_pg_data['h'] . ' port=' . $columby_pg_data['p'] . ' dbname=' . $columby_pg_data['db'] . ' user=' . $columby_pg_data['u'] . ' password=' . $columby_pg_data['pass'] .  ' connect_timeout=5'; 
  //message("connection $conn_string");
  $conn = pg_connect($conn_string);

  // Return connection resource or FALSE
  return $conn; 
}

/**
 * Close an open PostGIS connection
 * 
 * @param $c Connection
 *
 * @return close response
 *
 **/
function postgis_close($c) {
  return pg_close($c);
}

/**
 * Execute a postGIS query command
 * 
 * @param $sql String
 * 
 * @return result or FALSE
 *
 **/
function postgis_query($sql){

  $output = FALSE; 
  // open a connection
  $conn = postgis_connect();

  if (!$conn) {
    message("Error connecting to postGIS. ",'log','ERROR'); 
  } else {
    $result = pg_query($conn, $sql);
    if(!$result){
      message("Error executing query. [details: " . pg_last_error(). "]",'log','ERROR'); 
    }
    pg_close($conn); 
    $output = $result;
  }

  return $output;
}

// Export table to csv for download
function postgis_export_table($uuid) {
  $tablename = 'c'.str_replace('-', '_', $uuid);

  //Get table columns
  $sql ="SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='".$tablename."'";
  $result = postgis_query($sql);
  // process results
  $names = [];
  while ($row = pg_fetch_assoc($result)) {
    $names[] = $row['column_name'];
  }

  // remove unneeded columns and create string for new statement. 
  $n = "\"".implode("\",\"", array_diff($names, array('cid', 'createdAt', 'createdat','updatedAt','updatedat','the_geom')))."\""; 
  message("Column names: " . $n, 'log', 'NOTICE');

  // Create a local file ()
  $f = fopen("/home/columby/exports/".$uuid.".csv", 'w');
  if ($f != FALSE) {
    message('file: '. serialize($f), 'log', 'DEBUG');
    // Add first line (with the headers)
    $header = $n . ',"WKT"' . "\n";
    $fwrite = fwrite($f, $header); 

    if ($fwrite != FALSE){
      $sql = "SELECT " . $n . ",ST_AsText(the_geom) AS WKT FROM $tablename;";
      message("Sending sql: $sql",'log','DEBUG'); 
      $result = postgis_query($sql);
      $err = FALSE;
      while ($row = pg_fetch_assoc($result)) {
        $values = array();
        foreach ($row as $key=>$value) {
          $values[] = '"'.$value.'"';
        }
        $res = implode(",", $values) . "\n";
        message('values: ' . $res,'log','DEBUG');
        $fpc = file_put_contents("/home/columby/exports/".$uuid.".csv", $res, FILE_APPEND | LOCK_EX);
        if ($fpc == FALSE) {
          $err = TRUE;
          message("Error writing file. ", 'log','ERROR');  
        } else {
          message("written: " . $fpc, 'log', 'DEBUG');
        }
      }

      if (!$err){
        return true;
      } else {
        return false; 
      }
    } else {
      message("Error writing file. ", 'log','ERROR');  
    }
  } else {
    message("Error opening file. ", 'log','ERROR');
  }

  /*
  "Files named in a COPY command are read or written directly by the server, not by the client application. Therefore, they must reside on or be accessible to the database server machine, not the client. 
  They must be accessible to and readable or writable by the PostgreSQL user (the user ID the server runs as), not the client. 
  COPY naming a file is only allowed to database superusers, since it allows reading or writing any file that the server has privileges to access." (postgresql.org/docs/current/static/sql-copy.html)
  */

  // Copy creates the file on the database server, not the supervisor worker server. 
  /*
  $sql="COPY $tablename TO '/tmp/exports/".$uuid.".csv' WITH DELIMITER AS ',' NULL AS '' CSV HEADER;";
  message("Sending sql: $sql",'log','DEBUG'); 
  $result = postgis_query($sql);
  message("export table result:",'log','DEBUG'); 
  
  message(serialize($result),'log','DEBUG'); 
  if ($result != FALSE) { return TRUE; 
  } else { return FALSE;
  }
  */
}


/**
 * Open a Drupal Database connection
 * 
 * @return Connection or FALSE
 * 
 **/
function drupal_connect(){

  // get settings from settings.php
  global $drupaldb;
  // try connecting to the mysql db
  $conn = mysql_connect($drupaldb['h'], $drupaldb['u'], $drupaldb['pass']);

  if (!$conn) {
    // If there is an error in connecting, return nothing and report an error
    message("Error connecting to Drupal database: " . mysql_error(),'log','ERROR'); 
    return FALSE;
  } else {
    // Try selecting the right database
    $conndb = mysql_select_db($drupaldb['db'], $conn);
    if (!$conndb) {
      // if there is an error in selecting the database, return nothing and report
      message("Error selecting database: " . mysql_error(),'log','ERROR'); 
      return FALSE;
    }
  }

  // There is a connection and the database is selected, return this connection
  return $conn;
}

function drupal_close($conn){
}

function drupal_query($sql){
}

?>