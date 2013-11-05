<?php

include("lib/IronCore.class.php");
include("lib/IronWorker.class.php");

$worker = new IronWorker();
$worker->debug_enabled = true;

$payload = array(
  'uuid'=>'uuid',
  'uid'=>'uid',
  'type'=>'type',
  'uri'=>'uri'
);

$task_id = $worker->postTask("DataProcessor", $payload);

# Wait for task finish
$details = $worker->waitFor($task_id);
print_r($details);

$log = $worker->getLog($task_id);
echo "Task log:\n $log\n";