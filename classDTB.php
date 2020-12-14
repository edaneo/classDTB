<?php
  
class DTB {
  private $conn = null;
  private $args = [];

  public function __construct($dtb, $dtbname = null){
    if (is_array($dtb)) {
      $this->hostname = $dtb["host"];
      $this->username = $dtb["user"];
      $this->password = $dtb["pass"];
      $this->dtbname = $dtb["dtb"];
    } else {
      $this->hostname = preg_replace("/^.*Data Source=(.+?);.*$/", "\\1", $dtb);
      $this->username = preg_replace("/^.*User Id=(.+?);.*$/", "\\1", $dtb);
      $this->password = preg_replace("/^.*Password=(.+?)$/", "\\1", $dtb);
      $this->dtbname = $dtbname;
    }
  }
  private function connect() {
    if ($this->conn) return;
    
    @$this->conn = new mysqli($this->hostname, $this->username, $this->password, $this->dtbname);
    $tries = 0;
    while (($this->conn->connect_errno) && ($tries < 10)) {
      sleep(rand(1,5));
      $tries++;
      @$this->conn=new mysqli($this->hostname, $this->username, $this->password, $this->dtbname);
    }
    if ($this->conn->connect_errno) {
      die ("Failed to connect to MySQL: (".$this->conn->connect_errno.") ".$this->conn->connect_error);
    }
    $this->conn->set_charset("utf8");  
  }
  public function disconnect() {
    $this->conn->close();
    
    $this->conn = null;
  }
  public function query(){
    $this->connect();

    $x = $this->escapeStringWithParams(func_get_args());
    return $this->conn->query($x);  
  }
  public function prepareQuery(){
    $q = $this->escapeStringWithParams(func_get_args());
    return $q;      
  }
  private function escapeStringWithParams ($args) {
    $this->args = $args;
    $x = array_shift($this->args);
    $x = preg_replace_callback ("$\?$", array($this, "escapeCallback") , $x, count($this->args));
    return $x;
  }
  private function escapeCallback($matches) {
    return $this->escapeString(array_shift($this->args));
  }
  public function fetch($x){
    return $x->fetch_assoc();  
  }
  public function insert ($x) {
    $this->connect();

    if (!is_array($x["set"]))                    throw new Exception ("DTB: INSERT needs (array)set");
    if (!$x["table"] || !is_string($x["table"])) throw new Exception ("DTB: INSERT needs (string)table");
    if (array_key_exists("update", $x) && is_array($x["update"])) {
      $q = "INSERT INTO " . $x["table"] . " SET " . $this->escapeArray(array_merge($x["set"], $x["update"])) . " ON DUPLICATE KEY UPDATE " . $this->escapeArray($x["update"]);
    } else {
      $q = "INSERT INTO " . $x["table"] . " SET " . $this->escapeArray($x["set"]);
    }
    if (array_key_exists("test", $x)) throw new Exception ("DTB test: " . $q);
    if ($this->conn->query($q)) {
      return ($this->conn->insert_id);
    } else {
      return false;
    }
  }
  public function update($x){
    $this->connect();

    if (!is_array($x["set"])) die ("UPDATE need array SET");
    if (!$x["where"]) die ("UPDATE need non-empty WHERE");
    if (!$x["table"]) die ("UPDATE need non-empty TABLE");
    if (is_array($x["where"])) {
      $x["where"] = $this->escapeStringWithParams($x["where"]);
    }
    $q = "UPDATE " . $x["table"] . " SET " . $this->escapeArray($x["set"]) . " WHERE " . $x["where"];
    if (array_key_exists("test", $x)) die($q);
    $this->conn->query($q);
  }
  public function delete($x){
    $this->connect();

    if (!$x["where"]) die ("DELETE need non-empty WHERE");
    if (!$x["table"]) die ("DELETE need non-empty TABLE");
    if (is_array($x["where"])) {
      $x["where"] = $this->escapeStringWithParams($x["where"]);
    }
    $q = "DELETE FROM " . $x["table"] . " WHERE " . $x["where"];
    if (array_key_exists("test", $x)) die($q);
    $this->conn->query($q);
  }
  public function escapeString($x){
    $this->connect();

    return $this->conn->real_escape_string($x);
  }
  public function getAssocTable(){
    $this->connect();

    $q = $this->escapeStringWithParams(func_get_args());
    $res = $this->conn->query($q);
    $rows = array();
    while ($row = $res->fetch_assoc()) array_push($rows, $row);
    return $rows;
  }
  public function getRow(){
    $this->connect();

    $q = $this->escapeStringWithParams(func_get_args());
    $res = $this->conn->query($q);
    $row = $res->fetch_assoc();
    return $row;
  }
  public function getEnumTable(){
    $this->connect();

    $q = $this->escapeStringWithParams(func_get_args());
    $res = $this->conn->query($q);
    $rows = array();
    while ($row = $res->fetch_row()) array_push($rows, $row);
    return $rows;
  }
  public function getSingleColumnTable(){
    $q = $this->escapeStringWithParams(func_get_args());
    $res = $this->conn->query($q);
    $rows = array();
    while ($row = $res->fetch_row()) array_push($rows, $row[0]);
    return $rows;
  }
  public function getValue(){
    $q = $this->escapeStringWithParams(func_get_args());
    $val = null;
    $arr = $this->getSingleColumnTable($q);
    if (count($arr)) $val = $arr[0];
    return $val;
  }
  public function fetchRowAndDecode() {
    $row = null;
    $q = $this->escapeStringWithParams(func_get_args());
    $res = $this->conn->query($q);
    if ($res) {
      $row = $res->fetch_assoc();
      $this->decodeRow($row);
    }
    return $row;
  }
  private function decodeRow(&$row) {
    foreach($row as $key => &$value) {
      if ($value && is_string($value) && (substr($value, 0, 1) == "{" || substr($value, 0, 1) == "[")) {
        try {
          $x = json_decode($value, true);
        } catch (Exception $e) {
          $x = null;
        }
        if ($x) $value = $x;
      } 
    }
  }
  private function escapeArray($arr){
    $noescape = [
      "NOW()",
      "CURRENT_TIMESTAMP",
      "NULL"
    ];

    $set = [];

    foreach ($arr as $key => $value) {
      if (substr($key,0,1) == "#") {
        $key = substr($key, 1);
      } else if (is_array($value)) {
        $value = "'" . $this->conn->real_escape_string(json_encode($value)) . "'";
      } else if (is_string($value)) {
        if (!in_array($value, $noescape)) {
          $value = "'" . $this->conn->real_escape_string($value) . "'";
        }
      } else if (is_null($value)) {
        $value = "NULL";
      } else if (!is_float($value) && !is_integer($value)) {
        die("all values need to be NULL, ARRAY, STRING, INTEGER or FLOAT " . json_encode($arr));
      }

      array_push($set, $key . "=" . $value);
    }

    return implode(", ", $set);
  }
  public function close(){
    $this->conn->close();
  }
  public function parseInt(&$input, $keys) {
    foreach ($input as $key => &$value) {
      if (in_array($key, $keys) !== false) {
        $value = (int)$value;
      }
    }
  }
  public function parseFloat(&$input, $keys) {
    foreach ($input as $key => &$value) {
      if (in_array($key, $keys) !== false) {
        $value = (float)$value;
      }
    }
  }
  public function parseJSON(&$input, $keys) {
    foreach ($input as $key => &$value) {
      if (in_array($key, $keys) !== false) {
        try {
          $value = json_decode($value, true);
        } catch (Exception $e) {
          $value = null;
        }
      }
    }
  }
  public function getAffectedRows() {
    return $this->conn->affected_rows; 
  }
}

?>
