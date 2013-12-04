<?php

class Job {
  public $queue_table = 'columby_queue';
  public $job_data;
  public $conn; 

  function __construct(){
    // connect to the drupal CMS database
    $this->conn = drupal_connect(); 
    // Select all data from the oldest queue row which is undone and not processing
    $sql="SELECT 
      cq.ID, cq.uuid, n.nid, f.field_data_type_value as type 
      FROM columby_queue as cq 
      LEFT JOIN node as n ON 
        cq.uuid=n.uuid 
      LEFT JOIN field_data_field_data_type as f ON 
        n.nid=f.`entity_id`
      WHERE cq.done=0 
      ORDER BY cq.ID ASC 
      LIMIT 10;"
    ;
    
    if ($this->conn) {
      $result = mysql_query($sql, $this->conn) or die (mysql_error());  
      $this->job_data = mysql_fetch_assoc($result);
      message("Fetched job data: ID: ".$this->job_data['ID'] . ", uuid: ".$this->job_data['uuid'].", nid: ".$this->job_data['nid'].", type: ".$this->job_data['type'].".");
    }
  }


  function put($col,$val) {
    $table = $this->queue_table;
    $id = $this->job_data['ID'];
    $val = addslashes($val);
    mysql_query("UPDATE $table SET $col='$val' WHERE ID='$id'",$this->conn) or die(mysql_error());
    $this->array[$col] = $val;
  }

  function reset(){
    $this->put("done","0");
    $this->put("split","0");
    $this->put("processed","0");
  }
}

?>
