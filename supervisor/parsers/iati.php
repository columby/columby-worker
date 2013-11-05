<?php 

/** 
 * CSV Processor Class
 **/
class iati {
	
	private $q; 
	
	function __construct($q){ 
		
		$this->q = $q; 

		message("Starting new IATI parse job."); 

		// Currently IATI is not parsed, it is simply saved as a file to disk. 
		// So no parsing at this point. 

		
		// Finish up parsing
		$this->dbq_put("processing","0");
    $this->dbq_put("done","1");
    $this->dbq_put("error", NULL);

    message("FINISHED parsing: No errors.");

  }
  


  /** -------------- Queue table functions -------------- **/
  function dbq_put($col,$val){
    $table = $this->q->queue_table;
    $id = $this->q->array['ID'];
    $val = addslashes($val); 
    mysql_query("UPDATE $table SET $col='$val' WHERE ID='$id'",$this->q->connection) or die(mysql_error());
    $this->q->array[$col] = $val;

    message("Queue column '$col' updated to '" . substr($val, 0, 100) . " (sample)"); 
  }

}