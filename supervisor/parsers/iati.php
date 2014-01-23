<?php 

/** 
 * CSV Processor Class
 **/
class iati {
	
	public $worker_error;   // Text with error to send to API
  public $worker_status;  // Status of the worker

  private $job;           // incoming job item
  private $connection;    // connection to drupal database
  private $id;            // reference of current job id

	
	function __construct($job){ 
		
		$this->job = $job;

		message("IATI: Starting new parse job.",'log','NOTICE'); 

		// Currently IATI is not parsed, it is simply saved as a file to disk. 
		// So no parsing at this point. 
		$this->connection = drupal_connect();
    if (!$this->connection){
      message("IATI: Error connecting to the job queue database. ",'log','ERROR');
    } else {
      message("IATI: Connected to the job queue database. ",'log','NOTICE');
      $this->job = $job; 
      $this->id = $job['ID'];

      message("IATI: Fetching data for uuid: " . $job['uuid'],'log','NOTICE');
      message("IATI: Fetching data for queue id: " . $job['ID'],'log','NOTICE');

      // Finish up parsing
      $this->update_queue("processing","0");
      $this->update_queue("done","1");
      $this->update_queue("error", NULL);
      
      // End of processing data, update status
      $el = get_errorlog();
      if (count($el) > 0) {
        $el = implode("\n", $el);
        $this->update_queue('error', $el);
        message("IATI: FINISHED parsing: There where errors: $el",'log','DEBUG');
      } else {
        message("IATI: FINISHED parsing: No errors.",'log','NOTICE');
      }

      return TRUE;
    }

  }
  


  /** -------------- Queue table functions -------------- **/
  function update_queue($col, $val) {

    $conn = $this->connection; 
    $id = $this->id; 
    $val = addslashes($val); 
    $sql = "UPDATE columby_queue SET $col='$val' WHERE ID=$id"; 
    message("Setting queue item: $sql",'log','DEBUG');
    $this->job[$col] = $val;  
    $q = mysql_query($sql, $conn); 
    if (!$q) {
      message("Error: ".mysql_error(),'log','NOTICE');
    } else {
      message("Item written to queue-table ($col: $val)",'log','DEBUG');
    }
  }
}