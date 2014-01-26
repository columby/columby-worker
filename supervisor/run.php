#!/usr/bin/env php
<?php

// include the queue handler:
include("functions.php");

// This is mandatory to use the UNIX signal functions:
// http://php.net/manual/en/function.pcntl-signal.php
declare(ticks = 1);

// A function to write on the error output
function warn($msg){
  $stderr = fopen("php://stderr", "w+");
  fputs($stderr, $msg);
}

// Callback called when you run `supervisorctl stop'
function sigterm_handler($signo){
  warn($signo . "Columby worker stopped. \n");
  exit(0);
}

function main(){
  while (true) {
    // $d = date("F j, Y, g:i a");
    //sendTweet('Checking for queue, '.$d);
    while(check_queue()){
      process_job();
    }
    // sleep 10 sec
    echo (date('c') . " - sleep a minute \n");
    sleep(60);
  }
}

// Bind our callback on the SIGTERM signal and run the daemon
pcntl_signal(SIGTERM, "sigterm_handler");
main();

?>