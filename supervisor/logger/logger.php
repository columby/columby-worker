<?php

class Logger {
  
  public $log;
  
  function __construct(){
    $this->log = array();
    echo "Log instance initiated";
  }

  public function add($message){
    echo $message . "\n";
    $this->log[] = $message; 
  }

  public function get_log(){
    return $this->log; 
  }  

  public function clear(){
    $this->log = [];
  }
}

?>
