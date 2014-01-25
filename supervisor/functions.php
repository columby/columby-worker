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
$processing=FALSE;  // Is there a job running?
$job = array();          // Array with job information
$logger; 

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

  global $processing, $job;

  // initiate logger
  global $logger;
  $logger = new Logger(); 

  // Start with clean job-object
  $job = [];
  
  // Set processing car to true, since processing can take longer than the check_queue interval
  $processing = TRUE;
  message("---------------------------------- Start processing data ---------------------------------- ",'log','NOTICE');
  
  // Initialize api
  message("Connecting to the Drupal API ... ",'log','NOTICE');
  global $drupaluser;
  $api = new DrupalREST($drupaluser['endpoint'], $drupaluser['username'], $drupaluser['pass']);
  // get the required csrf token
  $api_token = $api->request_token();
  message('API CSRF Token: ' . $api_token,'log','NOTICE');
  // connect to the api
  $api_connection = $api->connect(); 
  message('API Connection user id: ' . $api_connection['uid'],'log','NOTICE');
  // log in if user is not connected
  $uid = $api_connection['uid'];
  if ($uid === 0) {
    message('Columby worker is not logged in via API. Logging in.','log','NOTICE');
    $api_login = $api->login();
    $uid = $api_login['uid'];
    message('Columby worker is logged in with uid: ' . $uid,'log','NOTICE');
  } else {
    message('Columby worker is logged in with uid: ' . $uid,'log','NOTICE');
  }
  
  if ($uid === 0){
    // not connected to the api, stop the process. 
    message('Error connecting to the Columby API','log','DEBUG');
    return false; 
  }


  //** CREATE JOB ITEM **//
  // Create a new queue with the new item to be processed inside.
  $retrieved_job = new Job();
  $job = $retrieved_job->job_data; 
  // uuid of the dataset node
  $uuid = $job['uuid'];
  // postgis tablename for the dataset
  $tablename = "c".str_replace("-", "_", $uuid);

  // update CMS Node fields (using columby service)
  // Tell Drupal that node data is being processed. 
  // $field_data_worker_status = 'in_progress';
  $fields=['worker_status'=>'in progress'];
  $r = $api->update($uuid, $fields); 
  message(print_r($r),'log','NOTICE');
  message("Sent 'in progress' worker status to the ColumbyAPI. ",'log','NOTICE');
  

  //** DROP EXISTING TABLE **//
  // Drop the table if it exists
  $sql = "DROP TABLE IF EXISTS $tablename";
  $result = postgis_query($sql);
  if (!$result) {
    message("Error connecting to the database. Not processing the data. Trying again in 60 seconds.",'log','ERROR'); 
    sleep(60);
  } else {

    message("Cleared existing database. ",'log','NOTICE');

    //** RESET QUEUE ITEM **//
    $retrieved_job->put("data","");
    $retrieved_job->put("stats","");
    $retrieved_job->put("split","0");
    $retrieved_job->put("total","");

    // Determine the job-type and start processing (separate classes)
    $type = $job['type']; 
    
    switch ($type) {
      /* 
        0 = "csv"
        1 = "arcgis10"
        2 = "iati"; 
      */
      case "0": //'csv':
        require_once("parsers/csv.php");
        message("Job type: CSV. ",'log','NOTICE');
        $parsed = new csv($job);
      break;
      case "1": //'arcgis10':
        require_once("parsers/arcgis.php");
        message("Job type: ARCGIS. ",'log','NOTICE');
        $parsed = new scraper($job);
      break;
      case "2": //'iati':
        require_once("parsers/iati.php");
        message("Job type: IATI. ",'log','NOTICE');
        $parsed = new iati($job);
      break;
      case null: 
      $parsed = '';
        message("No job-type found, skipping this task. ",'log','NOTICE');
      break; 
    }

    // Export table to csv-file if necessary
    if (($type == "0")||($type == "1")){
      message("Exporting table to file.",'log','NOTICE');
      $file = postgis_export_table($uuid);
      if ($file) {
        message("Export succeeded. ",'log','NOTICE');
        // Send command to Columby API
        $fields = [];
        $fields['file'] = '/home/columby/exports/'.$uuid.'.csv';
        if ($api->update($uuid, $fields)){
          message("Updated the file to " . $fields['file'],'log','NOTICE');
        }
      } else {
        message("Export error. ",'nolog','log','NOTICE');
      }
    } else {
      message("File is not saved as a table in the database, file-export is not needed. ",'log','NOTICE');
    }

    message("Finished processing job " . $uuid . ' with type ' . $type,'log','NOTICE');
  }

  // update CMS Node fields (using columby service)
  //$field_data_geo
  
  //$field_data_worker_status
  $fields=[];
  $fields['worker_status'] = 'finished';
  //message($parsed->geo,'log','NOTICE');

  $geo = $parsed->geo;
  if ($geo=='1') {
    $geo = 1; 
  } else {
    $geo = 0; 
  }
  message("Setting geo to ".$geo, 'log','NOTICE');
  $fields['geo'] = $geo;

  // check and log errors
  $e = $logger->get_errorlog();

  if (count($e)>0) {
    $fields['worker_status'] = 'error';
    $e = implode("\n", $e);
    message("There where errors generated during processing:",'log','NOTICE'); 
    message($e,'log','NOTICE');
    $fields['worker_error'] = $e;

  } else {
    message("No errors found during processing. Success!",'log','NOTICE');
  }
  
  // add sync data for API
  if (isset($parsed->sync_date)){
    $fields['sync_date'] = $parsed->sync_date;
  }
  // Send all fields to the API
  $r = $api->update($uuid, $fields);
  message(print_r($r),'log','NOTICE');
  message("Updated the node worker status to " . $fields['worker_status'],'log','NOTICE');

  message("---------------------------------- ------------------------ ---------------------------------- ",'log','NOTICE');
  message("---------------------------------- Finished processing data ---------------------------------- ",'log','NOTICE');
  message("---------------------------------- ------------------------ ---------------------------------- ",'log','NOTICE');

  worker_log_save($job['uuid']);

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
    message("render already running. ",'log','NOTICE');
    return FALSE; 
  }

  // connect to the Drupal database (returns FALSE or connection-resource)
  $conn = drupal_connect();

  // query when there is a connection
  if (!$conn) {
    message("Error connecting to the CMS database.",'log','NOTICE');
  
  } else {
    // To be sure, set the processing of items to off.
    message("Setting all items to processing=0",'log','DEBUG'); 
    $q = mysql_query("UPDATE columby_queue SET processing=0 WHERE processing=1", $conn);

    // check to see if there is (more than) 1 item in the queue for processing
    $q = mysql_query("SELECT COUNT(ID) FROM columby_queue WHERE done=0 AND processing=0", $conn);
    $count = mysql_result($q, 0);

    if ($count == 1) {
      message("There is $count job waiting for processing.",'log','DEBUG'); 
      return TRUE;
    } elseif ($count > 1) {
      message("There are $count job-items waiting for processing.",'log','DEBUG'); 
      return TRUE;
    } else {
      message("There are no items to be processed. ",'log','DEBUG'); 
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

function get_log(){
  global $logger;
  return $logger->get_log(); 
}

function get_errorlog(){
  global $logger;
  return $logger->get_errorlog();
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

  message('Saving log for $uuid', 'log', 'DEBUG');

  // only save the log for the right uuid
  if ($job['uuid'] == $uuid) {
    message('getting log object','log','DEBUG');
    $l = $logger->get_log();
    message('logcount: ' . count($l),'log','DEBUG');
    //message(implode(',',$l),'log','DEBUG');
    // check if there are log items present
    if (count($l) > 0) {
      
      // connect to the database
      $conn = drupal_connect();
      // convert array to string
      $log = implode("\n", $l);
      // escape unwanted characters
      $log = mysql_real_escape_string($log);
      
      // send to database
      $sql = "INSERT INTO columby_worker_log (uuid, log) VALUES ('$uuid', '$log')";
      message('Sending sql: ' . $sql,'log','DEBUG');

      $result = mysql_query($sql,$conn) or die(mysql_error());
      message("Log for $uuid sent to database. ",'log','DEBUG');
    } else {
      message("There are no items in the log, not sent to database. ",'log','DEBUG'); 
    }
  } else {
    message("$uuid is not the current log uuid. ",'log','DEBUG'); 
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



// ***** TWITTER ***** //

// Twitter API
require 'lib/tmhOAuth/tmhOAuth.php';
require 'lib/tmhOAuth/tmhUtilities.php';
function sendTweet($msg) {
  $tmhOAuth = new tmhOAuth(array(
        'consumer_key' => $twitterSettings['consumer_key'], 
        'consumer_secret' => $twitterSettings['consumer_secret'],
        'user_token' => $twitterSettings['user_token'],
        'user_secret' => $twitterSettings['user_secret'],
    ));

    $code = $tmhOAuth->request('POST', $tmhOAuth->url('1.1/statuses/update'), array(
        'status' => $msg
    ));
    if ($code == 200) {
    tmhUtilities::pr(json_decode($tmhOAuth->response['response']));
  } else {
      tmhUtilities::pr($tmhOAuth->response['response']);
  }
}




/*****

run()
  while checkqueue()>>
    render()>>
  sleep(60)

check_queue()
  drupal_connect()
    return FALSE or $conn
  count queue items
  return TRUE or FALSE

process_job() 
  create queue >> class queue()
  drop existing table
  determine job-type
  create job-type (class): arcgis or csv >> class csv()
  process metadata
    delete existing metadata
    add metadata

class queue() 
  Get first item from render_queue and return this array

CSV()
  Create the tablename from the render-queue array uuid


*****/

?>