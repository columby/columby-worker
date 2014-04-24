<?php 

class ColumbyAPI {

  var $endpoint;
  var $username;
  var $password;
  var $session;
  var $user;
  var $csrf_token;

  function __construct($endpoint, $username, $password) {

    $this->username = $username;
    $this->password = $password;
    $this->endpoint = $endpoint;
  }

  // Get the required csrf-token
  function request_token(){
    echo "Requesting token: ". $this->endpoint ."user/token.json \n";

    $d=array('some'=>'data');
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $this->endpoint . 'user/token.json');
    curl_setopt($ch, CURLOPT_POST, 1);
    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
    curl_setopt($ch, CURLOPT_HEADER, 0);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $d);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array(
      "Accept: application/json",
      "Content-Type: application/json",
      "Cookie:" . $this->session
    ));
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);

    $response = json_decode(curl_exec($ch),true);
    
    if ($response) {
      $this->csrf_token = (array_key_exists('token', $response)) ? $response['token'] : false;
    } else {
      echo 'Curl error: ' . curl_error($ch);
    }

    curl_close($ch);
    
    return $this->csrf_token;
  }

  // Connect to the columby API to check login status and api availability
  function connect() {
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $this->endpoint . 'system/connect.json');
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, '{}');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
    curl_setopt($ch, CURLOPT_HEADER, false);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array(
      "Accept: application/json",
      "Content-Type: application/json",
      "X-CSRF-Token: " . $this->csrf_token
    ));
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

    $response = json_decode(curl_exec($ch),true);
    curl_close($ch);
    
    if (isset($response['session_name'])){
      $this->session = $response['session_name'] . '=' . $response['sessid'];
    }

    if (isset($response['user'])) {
      $this->user = $response['user'];  
    }

    return $this->user;
  }

  // Login
  function login() {

    $ch = curl_init();
    $post_data = array(
      'username' => $this->username,
      'password' => $this->password,
    );
    $post_data = http_build_query($post_data, '', '&');

    curl_setopt($ch, CURLOPT_URL, $this->endpoint . 'user/login.json');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HEADER, false);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $post_data);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array (
      "Accept: application/x-www-form-urlencoded",
      "Content-Type: application/x-www-form-urlencoded",
      "X-CSRF-Token: " . $this->csrf_token
    ));

    $response = json_decode(curl_exec($ch),true);
    if (isset($response[0])){
      echo "Login response: " . $response[0] . "\n";
    }

    if ( isset($response['session_name']) && isset($response['sessid']) ) {
      $this->session = $response['session_name'] . '=' . $response['sessid'];
    }
    if (isset($response['user'])){
      $this->user = $response['user'];
      echo "Login uid: " . $response['user']['uid'] . "\n"; 
    }
    // get new token
    echo "Requesting new token after logging in. \n";
    $token = $this->request_token();

    if (isset($response['user']['uid'])){
      return $response['user']['uid'];
    } else {
      return FALSE;
    }
  }

  // Retrieve single node data
  function node_retrieve($uuid){
    
    $url = $this->endpoint . "dataset/" . $uuid . ".json";
    
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array (
      "Accept: application/json",
      "Content-type: application/json",
      "Cookie: ". $this->session,
      "X-CSRF-Token: " .$this->csrf_token
    ));

    $result = json_decode(curl_exec($ch),true);

    curl_close($ch);
    
    return $result;
  }

  // Update node worker status
  function node_update($uuid, $data){
    $ch = curl_init();
    $post_data = $data;
    $post_data = http_build_query($post_data, '', '&');

    echo 'post data: ' . $post_data. "\n";

    curl_setopt($ch, CURLOPT_URL, $this->endpoint . 'ironworker/update-node/'. $uuid .'.json');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HEADER, false);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $post_data);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array (
      "Accept: application/x-www-form-urlencoded",
      "Content-Type: application/x-www-form-urlencoded",
      "X-CSRF-Token: " . $this->csrf_token,
      "Cookie: " . $this->session
    ));

    $response = json_decode(curl_exec($ch),true);
    print_r($response);

    curl_close($ch);

    return $response;
  }

}

?>