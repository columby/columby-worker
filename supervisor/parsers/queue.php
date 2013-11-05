<?php

class queue {

  public $queue_table = "columby_queue";
  public $array;  // queue list

  function __construct(){

    // connect to the drupal CMS database
    $this->connection = drupal_connect(); 
    // Select all data from the oldest queue row which is undone and not processing
    //$sql = "SELECT * FROM columby_queue WHERE done=0 AND processing=0 ORDER BY ID ASC LIMIT 1";
    // Get data from node, not from queue
    $sql="SELECT cq.ID, cq.uuid, n.nid, f.field_data_type_value as type FROM columby_queue as cq LEFT JOIN node as n ON cq.uuid=n.uuid LEFT JOIN field_data_field_data_type as f ON n.nid=f.`entity_id`WHERE cq.done=0 ORDER BY cq.ID ASC LIMIT 10";
    $res = mysql_query($sql,$this->connection) or die (mysql_error());

    // Add all data queue object. (uuid, type); 
    $this->array = mysql_fetch_array($res);

    // If there is no nid returned, do not process it. 
    $this->put('done', "1");
    $this->put('processing', "0");
  }

  function put($col,$val){
    $table = $this->queue_table;
    $id = $this->array['ID'];
    $val = addslashes($val);
    mysql_query("UPDATE $table SET $col='$val' WHERE ID='$id'",$this->connection) or die(mysql_error());
    $this->array[$col] = $val;
  }

  function reset(){
    $this->put("done","0");
    $this->put("split","0");
    $this->put("processed","0");
  }
}

?>