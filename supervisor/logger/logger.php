<?php

class Logger {
  
  public $log; 
  public $errorlog;

  /*
  ERROR: Error conditions.
  NOTICE: (default) Normal but significant conditions.
  DEBUG: Debug-level messages.
  */

  function __construct(){
    $this->log = array();
    message('Log instance initiated','log','DEBUG');
    
    $errorlog = array();
    message('Error log instance initiated','log','DEBUG');
  }


  public function add($message, $severity){
    
    if ($severity=='NOTICE'){
      $this->log[] = $message;
      message("Message added to log: $message",'log','DEBUG');
      //message("Log count: " . count($this->log),'log','DEBUG');
    
    } elseif ($severity=='ERROR'){
      $this->errorlog[] = $message;
      message('Message added to error log: ' . $message,'log','DEBUG');
      message("Error log count: " . count($this->errorlog),'log','DEBUG');
    }
  }


  public function get_log(){
    return $this->log; 
  }
  public function get_errorlog(){
    return $this->errorlog;
  }


  public function clear(){
    $this->log = [];
    message('Log cleared','log','DEBUG'); 
  }

  public function clear_errors(){
    $this->errorlog = [];
    message('Error log cleared','log','DEBUG'); 
  }
}

?>
