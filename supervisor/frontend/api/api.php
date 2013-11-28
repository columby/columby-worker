<?php

$command = $_GET['command']; 

switch ($command) {
  case 'view-log': 

  break; 
  
  case 'start-supervisor': 
    exec('service supervisord start');
    echo 'started!';
  break; 

  case 'stop-supervisor': 
    exec('service supervisord stop', $output);
    echo print_r($output);
  break; 
}

$filename = "/var/log/supervisor/supervisord.log";
$offset = $_GET['offset']; 

//session_start();
$handle = fopen($filename, 'r');
$data = stream_get_contents($handle, -1, $offset);
$offset = ftell($handle); 

$data = nl2br($data);
$arr = array('message' => $data, 'offset' => $offset, 'status'=> $output[0]); 
echo json_encode($arr);


?>