<?php

error_reporting(E_ALL);

require_once("config/settings.php");
require_once("parsers/queue.php");


// RENDER
$processing=FALSE;
$worker_log='';

function render(){

  global $processing, $worker_log;

  $processing = TRUE;

  message("---------------------------------- Start processing data ---------------------------------- ");
  
  // Create a new queue with the new item to be processed inside.
  $q = new queue();
  // uuid of the dataset node
  $uuid = $q->array['uuid'];
  // postgis tablename for the dataset
  $tablename = "c".str_replace("-", "_", $uuid);

  // Drop the table if it exists
  $sql = "DROP TABLE IF EXISTS $tablename";
  $res = pgq($sql, TRUE);
  message("Cleared existing database if it existed.");

  // Reset queue
  $q->put("data","");
  $q->put("stats","");
  $q->put("split","0");
  $q->put("total","");

  // Determine the job-type and start processing (separate classes)
  message("Determining job-type");
  $type = $q->array['type']; 
  switch ($type) {
    /* 
      0 = "csv"
      1 = "arcgis10"
      2 = "iati"; 
    */

    case 0: //'csv':
      require_once("parsers/csv.php");
      message("Creating a new CSV job-type. ");
      $a = new csv($q);
    break;
    case 1: //'arcgis10':
      require_once("parsers/arcgis.php");
      message("Creating a new ARCGIS job-type. ");
      $a = new scraper($q);
    break;
    case 2: //'iati':
      require_once("parsers/iati.php");
      message("Creating a new IATI job-type. ");
      $a = new iati($q);
    break;
    case null: 
      message("No record found, skipping this task. ");
    break; 
  }

  message("Finished processing ".$q->array['type']." of ".$q->array['uuid']);

  worker_log_save($q->array['uuid'], 1, 'message');

  $processing = FALSE;
}

// check queue
function check_queue(){
  
  global $processing; 
  if ($processing) {
    message("render already running. "); 
    return FALSE; 
  }

  // connect to the Drupal database (returns FALSE or connection-resource)
  $conn = drupal_connect();

  // query when there is a connection
  if ($conn) {
    // We already checked for processing above, is this secure??? 
    // Set old items still set to processing=0
    message("Setting all items to processing=0"); 
    $q = mysql_query("UPDATE columby_queue SET processing=0 WHERE processing=1",$conn);

    // check to see if there is (more than) 1 item in the queue for processing
    $q = mysql_query("SELECT COUNT(ID) FROM columby_queue WHERE done=0 AND processing=0",$conn);
    $count = mysql_result($q,0);

    // For duplicate entries, delete old items
      // Get the next item for processing (lowest ID where done=0)
    //$q = mysql_query("SELECT ID, UUID FROM columby_queue WHERE done=0 AND processing=0 ORDER BY ID ASC LIMIT 1",$conn);
      // Check if there are more jobs for one UUID
    //$q = mysql_query("SELECT ID, UUID FROM columby_queue WHERE done=0 AND processing=0 AND UUID='$uuid' ORDER BY ID ASC OFFSET 1",$conn);
      // Update all older items and set them to done=1
    //$q = mysql_query("UPDATE columby_queue SET processing=0, done=1, error='Aborted because of new job. ' WHERE UUID='$uuid' AND done=0 ORDER BY ID ASC OFFSET 1",$conn);

    if ($count > 0) {
      message("There are items to be processed: $count");
      return TRUE;
    } else {
      message("There are no items to be processed. ");
    }
  }

  // error in connection, or no items to process
  return FALSE;
}

// Messages
function message($message,$service="log"){

  switch ($service){
    case "log":
      echo date('c')." - ".$message."\n";

      // Add to log object to be able to send to columby_worker_log_log()
      worker_log_add(date('c')." - ".$message."\n");

      break;
    case "tweet":
      echo "TWEET: $message \n";
      break;
    case "drupal":
      warn($message);
      break;
  }
}


// *** Log functions ***//
function worker_log_add($message){
  global $worker_log;
  $worker_log .= date('c')." - ".$message."\n";
}

function worker_log_save($uuid, $severity, $message){
  global $worker_log;
  $conn = drupal_connect();
  $worker_log = mysql_real_escape_string($worker_log);
  $message = mysql_real_escape_string($message);
  mysql_query("INSERT INTO columby_worker_log (uuid, severity, message, log) VALUES ('$uuid', $severity, '$message', '$worker_log')",$conn) or die(mysql_error());

  message('worker_log: ' . $worker_log);
}




// ***** PostGIS ***** //

// PostGIS Connection
function columby_postgis_connect() {

  global $columby_pg_data;

  $conn_string = 'host='.$columby_pg_data['h'] . ' port=' . $columby_pg_data['p'] . ' dbname=' . $columby_pg_data['db'] . ' user=' . $columby_pg_data['u'] . ' password=' . $columby_pg_data['pass'] .  ' connect_timeout=5'; 
  //message("connection $conn_string");
  $conn = pg_connect($conn_string);

  // Return connection resource or FALSE
  return $conn; 
}
// PostGIS close
function columby_postgis_close($c) {
  return pg_close($c);
}

// PostGis query + error log
function pgq($sql,$no_error=FALSE){

  $conn = columby_postgis_connect();

  if ($conn) {

    $q = pg_query($conn, $sql);
    if(!$q && !$no_error){
      message(pg_last_error());
      pg_close($conn); 
      return false;
    }
    // Close the connection
    pg_close($conn); 

    // send the query result resource back. 
    return $q;
  } else {
    message("Error connecting to postGIS. "); 
    sleep(60); 
    return FALSE; 
  }
}

// Drupal Connection - FALSE
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
      $conndb = mysql_select_db($drupaldb['db'],$conn);
      if (!$conndb) {
        // if there is an error in selecting the database, return nothing and report
        message("Error selecting database: " . mysql_error());
        return FALSE;
      }
    }

  // There is a connection and the database is selected, return this connection
  return $conn;
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

checkqueue()
  drupal_connect()
    return FALSE or $conn
  count queue items
  return TRUE or FALSE

render() 
  create queue >> class queue()
  drop existing table
  determine job-type
  create job-type (class): arcgis or csv >> class csv()
  process metadata
    delete existing metadata
    add metadata

queue() 
  Get first item from render_queue and return this array

CSV()
  Create the tablename from the render-queue array uuid


*****/

?>