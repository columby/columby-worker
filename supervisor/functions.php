<?php

/** 
 *
 * Background data processor
 * 
 **/

error_reporting(E_ALL);

require_once("config/settings.php");
require_once("parsers/queue.php");

// Set vars
$processing=FALSE;  // Is there a job running?
$job = array();          // Array with job information
$job['job_log'] = array();
$job['job_errorlog'] = array();

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

  // Start with clean job-object
  $job = [];
  $job['job_log'] = array();

  // Set processing car to true, since processing can take longer than the check_queue interval
  $processing = TRUE;
  message("---------------------------------- Start processing data ---------------------------------- ");
  
  //** CREATE JOB ITEM **//
  // Create a new queue with the new item to be processed inside.
  $retrieved_job = new Job();
  $job = $retrieved_job->job_data; 
  // uuid of the dataset node
  $uuid = $job['uuid'];
  // postgis tablename for the dataset
  $tablename = "c".str_replace("-", "_", $uuid);

  //** DROP EXISTING TABLE **//
  // Drop the table if it exists
  $sql = "DROP TABLE IF EXISTS $tablename";
  $result = postgis_query($sql);
  if (!$result) {
    message("Error connecting to the database. Not processing the data. Trying again in 60 seconds."); 
    sleep(60);
  } else {

    message("Cleared existing database if it existed.");

    //** RESET QUEUE ITEM **//
    $retrieved_job->put("data","");
    $retrieved_job->put("stats","");
    $retrieved_job->put("split","0");
    $retrieved_job->put("total","");

    // Determine the job-type and start processing (separate classes)
    $type = $job['type']; 
    message("Determining job-type: " . $type);
    
    switch ($type) {
      /* 
        0 = "csv"
        1 = "arcgis10"
        2 = "iati"; 
      */
      case "0": //'csv':
        require_once("parsers/csv.php");
        message("Creating a new CSV job-type. ");
        $parsed = new csv($job);
      break;
      case "1": //'arcgis10':
        require_once("parsers/arcgis.php");
        message("Creating a new ARCGIS job-type. ");
        $parsed = new scraper($job);
      break;
      case "2": //'iati':
        require_once("parsers/iati.php");
        message("Creating a new IATI job-type. ");
        $parsed = new iati($job);
      break;
      case null: 
      $parsed = '';
        message("No record found, skipping this task. ");
      break; 
    }

    message("Finished processing job " . $job['uuid'] . ' with type ' . $type);

    $job['job_log'] = $parsed->job_log; 
    message(print_r($job));
    $job['job_errorlog'] = $parsed->job_errorlog; 
  }

  message("---------------------------------- Finished processing data ---------------------------------- ");
  message("---------------------------------------------------------------------------------------------- ");

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
    message("render already running. ");
    return FALSE; 
  }

  // connect to the Drupal database (returns FALSE or connection-resource)
  $conn = drupal_connect();

  // query when there is a connection
  if (!$conn) {
    message("Error connecting to the CMS database.");
  
  } else {
    // To be sure, set the processing of items to off.
    message("Setting all items to processing=0", "nolog"); 
    $q = mysql_query("UPDATE columby_queue SET processing=0 WHERE processing=1", $conn);

    // check to see if there is (more than) 1 item in the queue for processing
    $q = mysql_query("SELECT COUNT(ID) FROM columby_queue WHERE done=0 AND processing=0", $conn);
    $count = mysql_result($q, 0);

    if ($count == 1) {
      message("There is $count job waiting for processing.", "nolog");
      return TRUE;
    } elseif ($count > 1) {
      message("There are $count job-items waiting for processing.", "nolog");
      return TRUE;
    } else {
      message("There are no items to be processed. ", "nolog");
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
 *   Where to send the message to. 
 */
function message($message,$service="log"){

  switch ($service){
    // Only send to supervisor log
    case "nolog":
      echo date('c')." - ".$message."\n";
      break;
    // Send it to the log message object
    case "log":
      echo date('c')." - ".$message."\n";
      worker_log_add(date('c')." - ".$message."\n");
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


/************* WORKER LOG *************/

/**
 * Add a message to the worker log
 * 
 * @param $message String
 *
 **/
function worker_log_add($message){
  global $job;
  $job['job_log'][] = $message;
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
  $log = $job['job_log']; 
  $errorlog = $job['job_errorlog'];

  message('Saving log for $uuid', 'nolog');

  // only save the log for the right uuid
  if ($job['uuid'] == $uuid) {
    
    // check if there are log items present
    if (count($log) > 0) {
      
      // connect to the database
      $conn = drupal_connect();
      
      // convert array to string
      $log = implode("\n", $log);
      message($log, 'nolog');
      
      // escape unwanted characters
      $log = mysql_real_escape_string($log);
      message($log,'nolog');

      // send to database
      $result = mysql_query("INSERT INTO columby_worker_log (uuid, log) VALUES ('$uuid', '$log')",$conn) or die(mysql_error());
      message("Log for $uuid sent to database. ", "nolog");
    } else {
      message("There are no items in the log, not sent to database. ", "nolog");
    }
  } else {
    message("$uuid is not the current log uuid. ", "nolog");
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
 * @return pg_query return or false
 *
 **/
function postgis_query($sql){

  // open a connection
  $conn = postgis_connect();

  if (!$conn) {
    message("Error connecting to postGIS. "); 
    return FALSE; 

  } else {

    $result = pg_query($conn, $sql);
    
    if(!$result){
      message(pg_last_error());
      pg_close($conn); 
      return FALSE;
    }
    // Close the connection
    pg_close($conn); 
    // send the query result resource back. 
    return $result;
  }
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
    message("Error connecting to Drupal database: " . mysql_error());
    return FALSE;
  } else {
    // Try selecting the right database
    $conndb = mysql_select_db($drupaldb['db'], $conn);
    if (!$conndb) {
      // if there is an error in selecting the database, return nothing and report
      message("Error selecting database: " . mysql_error());
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