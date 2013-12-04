<?php 

class DrupalREST {

  var $username;
  var $password;
  var $session;
  var $endpoint;

  function __construct($endpoint, $username, $password) {

      $this->username = $username;
      $this->password = $password;
      $this->endpoint = $endpoint;
  }

  function login() {

    $ch = curl_init($this->endpoint . 'user/login.json');
    $post_data = array(
      'username' => $this->username,
      'password' => $this->password,
    );
    $post = http_build_query($post_data, '', '&');
    
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HEADER, false);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $post);
    curl_setopt($ch, CURLOPT_HTTPHEADER,array (
      "Accept: application/json",
      "Content-type: application/x-www-form-urlencoded"
    ));
    
    $response = json_decode(curl_exec($ch));

    //Save Session information to be sent as cookie with future calls
    if ($response->session_name && $response->sessid){
      $this->session = $response->session_name . '=' . $response->sessid;
    
      // GET CSRF Token
      curl_setopt_array($ch, array(
        CURLOPT_RETURNTRANSFER => 1,
        CURLOPT_URL => $this->endpoint . 'user/token.json',
      ));
      curl_setopt($ch, CURLOPT_COOKIE, "$this->session"); 
      
      $ret = new stdClass;
      $ret->response = json_decode(curl_exec($ch));
      $ret->error    = curl_error($ch);
      $ret->info     = curl_getinfo($ch);
      $t= $ret->response;
      $this->csrf_token = $t->token;
      if ($this->csrf_token){
        return true;
      }
    }
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
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "PUT"); // Do an UPDATE PUT POST
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, TRUE);
    curl_setopt($ch, CURLOPT_HEADER, TRUE);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $post);
    curl_setopt($ch, CURLOPT_HTTPHEADER,
    array (
      "Accept: application/json",
      "Content-type: application/x-www-form-urlencoded",
      "Cookie: $this->session",
      'X-CSRF-Token: ' .$this->csrf_token
    ));

    $result = $this->_handleResponse($ch);

    curl_close($ch);

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