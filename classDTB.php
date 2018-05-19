<?php
  
  class DTB {
  private $conn=null;
  public function __construct($dtb, $dtbname = null){
    if (is_array($dtb)) {
      $hostname = $dtb["host"];
      $username = $dtb["user"];
      $password = $dtb["pass"];
      $dtbname = $dtb["dtb"];
    } else {
      $hostname = preg_replace("/^.*Data Source=(.+?);.*$/", "\\1", $dtb);
      $username = preg_replace("/^.*User Id=(.+?);.*$/", "\\1", $dtb);
      $password = preg_replace("/^.*Password=(.+?)$/", "\\1", $dtb);
    }
    
    @$this->conn = new mysqli($hostname, $username, $password, $dtbname);
    $tries = 0;
    while (($this->conn->connect_errno) && ($tries < 10)) {
      sleep(rand(1,5));
      $tries++;
      @$this->conn=new mysqli($hostname, $username, $password, $dtbname);
    }
    if ($this->conn->connect_errno) {
      die ("Failed to connect to MySQL: (".$this->conn->connect_errno.") ".$this->conn->connect_error);
    }
    $this->conn->set_charset("utf8");  
  }
  private $args = array();
  public function query(){
    $x = $this->escapeStringWithParams(func_get_args());
    return $this->conn->query($x);  
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
  public function insert($x){
    if (!is_array($x["set"])) die ("INSERT need array SET");
    if (!$x["table"]) die ("INSERT need non-empty TABLE");
    if (array_key_exists("update", $x) && is_array($x["update"])) {
      $q="INSERT INTO ".$x["table"]." SET ".$this->escapeArray(array_merge($x["set"], $x["update"])) . " ON DUPLICATE KEY UPDATE " . $this->escapeArray($x["update"]);
    } else {
      $q="INSERT INTO ".$x["table"]." SET ".$this->escapeArray($x["set"]);
    }
    if (array_key_exists("test", $x)) die($q);
    $this->conn->query($q);
    return ($this->conn->insert_id);
  }
  public function update($x){
    if (!is_array($x["set"])) die ("UPDATE need array SET");
    if (!$x["where"]) die ("UPDATE need non-empty WHERE");
    if (!$x["table"]) die ("UPDATE need non-empty TABLE");
    if (is_array($x["where"])) {
      $x["where"] = $this->escapeWhereClausule($x["where"]);
    }
    $q = "UPDATE " . $x["table"] . " SET " . $this->escapeArray($x["set"]) . " WHERE " . $x["where"];
    if (array_key_exists("test", $x)) die($q);
    $this->conn->query($q);
  }
  public function delete($x){
    if (!$x["where"]) die ("DELETE need non-empty WHERE");
    if (!$x["table"]) die ("DELETE need non-empty TABLE");
    if (is_array($x["where"])) {
      $x["where"] = $this->escapeWhereClausule($x["where"]);
    }
    $q = "DELETE FROM " . $x["table"] . " WHERE " . $x["where"];
    if (array_key_exists("test", $x)) die($q);
    $this->conn->query($q);
  }
  public function escapeString($x){
    return $this->conn->real_escape_string($x);
  }
  public function getAssocTable($q){
    $res = $this->conn->query($q);
    $rows = array();
    while ($row = $res->fetch_assoc()) array_push($rows, $row);
    return $rows;
  }
  public function getEnumTable($q){
    $res = $this->conn->query($q);
    $rows = array();
    while ($row = $res->fetch_row()) array_push($rows, $row);
    return $rows;
  }
  public function getSingleColumnTable($q){
    $res = $this->conn->query($q);
    $rows = array();
    while ($row = $res->fetch_row()) array_push($rows, $row[0]);
    return $rows;
  }
  
  private function escapeArray($arr){
    $noescape=Array("NOW()","CURRENT_TIMESTAMP");
    $set=Array();
    foreach ($arr as $key=>$value) {
      if (substr($key,0,1)=="#") $key=substr($key,1);
      else if (is_string($value)) {
        if (!in_array($value, $noescape)) $value="'".$this->conn->real_escape_string($value)."'";
      } else if (is_null($value)) $value="NULL";
      else if (!is_float($value) && !is_integer($value)) die("all values need to be NULL, STRING, INTEGER or FLOAT ".json_encode($arr));
      array_push($set,$key."=".$value);
    }
    return implode(", ",$set);
  }
}

?>
