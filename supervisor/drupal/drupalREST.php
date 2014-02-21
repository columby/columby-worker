<?php 

class DrupalREST {

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
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $this->endpoint . 'user/token.json');
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, 'some=field');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_HEADER, false);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array(
      "Accept: application/json",
      "Content-Type: application/json",
      "Cookie:" . $this->session
    ));
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

    $response = json_decode(curl_exec($ch),true);
    curl_close($ch);
    $this->csrf_token = (array_key_exists('token', $response)) ? $response['token'] : false;
    
    return $this->csrf_token;
  }


  // Connect to the columby API to check login status and api availability
  function connect() {
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $this->endpoint . 'system/connect.json');
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, 'some=field');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_HEADER, false);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array(
      "Accept: application/json",
      "Content-Type: application/json",
      "X-CSRF-Token: " . $this->csrf_token
    ));
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

    $response = json_decode(curl_exec($ch),true);
    curl_close($ch);
    
    $this->session = $response['session_name'] . '=' . $response['sessid'];
    $this->user = $response['user'];

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
    if ($response['session_name'] && $response['sessid']){
      $this->session = $response['session_name'] . '=' . $response['sessid'];
    }
    if ($response['user']){
      $this->user = $response['user'];
    }
    // get new token
    $token = $this->request_token();

    return $this->user;
  }


  // Retrieve a node from a node id
  function retrieve($uuid) {

      //Cast node id as integer
      $uuid = (string) $uuid;
      $ch = curl_init($this->endpoint . 'worker/' . $uuid . '.json');
      curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
      curl_setopt($ch, CURLOPT_HEADER, TRUE);
      curl_setopt($ch, CURLINFO_HEADER_OUT, TRUE);
      curl_setopt($ch, CURLOPT_HTTPHEADER,array (
        "Accept: application/json",
        "Content-type: application/x-www-form-urlencoded",
        "Cookie: $this->session",'X-CSRF-Token: ' .$this->csrf_token
      ));

      $result = $this->_handleResponse($ch);

      curl_close($ch);

      return $result;
  }


  function update($uuid, $node) {

    $post = http_build_query($node, '', '&');
    $ch = curl_init($this->endpoint . 'worker/'.$uuid.'.json');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "PUT"); // Do an UPDATE PUT POST
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HEADER, FALSE);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $post);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array (
      "Accept: application/json",
      "Content-type: application/x-www-form-urlencoded",
      "Cookie: " . $this->session,
      'X-CSRF-Token: ' . $this->csrf_token
    ));

    $result = json_decode(curl_exec($ch),true);

    return $result;
  }


  // Private Helper Functions
  private function _handleResponse($ch) {

    $response = curl_exec($ch);
    $info = curl_getinfo($ch);

    //break apart header & body
    $header = substr($response, 0, $info['header_size']);
    $body = substr($response, $info['header_size']);

    $result = new stdClass();

    if ($info['http_code'] != '200') {
      $header_arrray = explode("\n",$header);
      $result->ErrorCode = $info['http_code'];
      $result->ErrorText = $header_arrray['0'];
    } else {
      $result->ErrorCode = NULL;
      $decodedBody= json_decode($body);
      $result = (object) array_merge((array) $result, (array) $decodedBody );
      $result = $body;
    }

    return $body;
  }
}
?>