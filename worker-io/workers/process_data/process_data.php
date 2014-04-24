<?php

// Parse configuration settings
$config_file = parse_ini_file('config.ini', true);

// Open Columby API Class file
require_once dirname(__FILE__) . '/columby_api/columby_api.class.php';

// variables
$payload = null;               // Incoming vars
$uuid = null;                  // UUID, received in payload vars
$env = null; 
$columby_api = null;           // connection to drupal database
$node = null;                  // Node object (retrieved from api)
$job_id = null;                // 
$worker_status = null;         // Status of the worker
$worker_error = null;          // Text with error to send to API


$payload = (array)getPayload();

if (!isset($payload['uuid'])){
  echo "Error: no uuid provided. \n";
  $worker_status = 'error';
  $worker_error = 'No uuid provided';
  handle_error();
}
$uuid = $payload['uuid'];
echo 'working with UUID: '. $uuid ."\n";

if (!isset($payload['env'])){
  echo "Error: no env provided. \n";
  $worker_status = 'error';
  $worker_error = 'No env provided';
  handle_error();
}
$env=$payload['env'];
echo "Working with environment: $env \n";

$config=array();
$config['columby'] = $config_file['columby_'. $env];
$config['postgis'] = $config_file['postgis_'. $env];

//print_r($config);

// Connect to the Columby API
echo "  --------------------------------------------  \n";
if ( !columby_api_connect() ){
  echo "Columby API: Problem in connecting to Columby API. \n";
  $worker_status = 'error';
  $worker_error = 'Error connecting to the Columby API.';
  handle_error();
}
echo "Columby API: Connected to the job queue database. \n";


// Fetch job info
echo "  --------------------------------------------  \n";
echo "Columby API: Fetching node info and datafile uri. \n";
if (!retrieve_node_info()){
  echo "Error fetching node information. \n";
  $worker_status = 'error';
  $worker_error = 'Error fetching node information.';
  handle_error();
}

// Determine job type
if (!isset($node['dataset']['data_type'])){
  echo "Error determining job type. \n";
  $worker_status = 'error';
  $worker_error = 'Error Error determining job type.';
  handle_error();
} 

echo "datatype: ". $node['dataset']['data_type'] ."\n";

switch ($node['dataset']['data_type']) {
  case 'csv':
    require_once dirname(__FILE__) . '/csv/process_csv.class.php';
    $job = new ProcessCSV($uuid,$config);
    $result = $job->start();
    if (isset($result['error'])) {
      handle_error($result);
    } else {
      handle_results($result);
    }
    break;
  case 'arcGIS':
    require_once dirname(__FILE__) . '/arcgis/process_arcgis.class.php';
    if (!isset($payload['cycle'])) {
      $payload['cycle'] == 250;
    }
    $vars = array(
      'uuid' => $uuid,
      'data_url' => $node['dataset']['data_url'],
      'config' => $config,
      'cycle' => $payload['cycle']
    );
    $job = new ProcessArcGIS($vars);
    $result = $job->start();
    if (isset($result['error'])) {
      handle_error($result);
    } else {
      handle_results($result);
    }
    break;
  case 'iati':
    $job = 'iati';
    echo "Iati job type: not processing at the moment. \n";
  break;
  default:
    $job = null;
    echo "No job type found. \n";
    handle_error();
    break;
}

echo "--- FINISHED --- \n";




/**** HELPERS ****/

function columby_api_connect(){
  global $config, $columby_api;

  $return = FALSE;

  $columby_api = new ColumbyAPI($config['columby']['endpoint'], $config['columby']['username'], $config['columby']['password']);
  
  // First get the required csrf token to make post requests
  if (!$api_token = $columby_api->request_token()){
    echo "Error getting csrf token \n";
    return FALSE; 
  }
  echo "Columby API: token: $api_token \n";
  
  // Connect to the api to get active user or anonymous
  $columby_conn = $columby_api->connect();
  
  // log in if user is not connected
  if ($columby_conn['uid'] === 0) {
    echo "Columby API: No active session, logging in... \n";
    $api_login = $columby_api->login();
    if (!$api_login) {
      echo "Columby API: Error connecting to the job queue database. \n";
      $return = FALSE;
    } else {
      echo "Columby API: Logged in with uid: " . $api_login . "\n";
      $return = TRUE;
    }  
  }
  
  return $return;
}

function retrieve_node_info(){
  
  global $columby_api, $node, $uuid;

  $result = FALSE;

  if ( $node = $columby_api->node_retrieve($uuid) ){
    $result = TRUE;
  }
  
  return $result; 
}

// finalizing the data processing loop.
// Send status update to the columby dataset node
function handle_results($result) {
  global $uuid, $columby_api;
  echo "Sending final result to Columby. \n";
  print_r($result);
  $result = $columby_api->node_update($uuid, $result);
  echo "\n";
  echo "........................ \n";

  return true;
}

// Handle worker error. Log and quit. 
function handle_error(){
  global $uuid, $columby_api, $worker_status, $worker_error;

  echo "Handling exit, worker status: $worker_status, worker_error: $worker_error \n";
  $columby_api->node_update($uuid, array(
    'worker_status' => $worker_status,
    'worker_error'  => $worker_error
  ));
  echo "Aborting the job now ... \n";
  echo "........................ \n";
  exit();
}

?>