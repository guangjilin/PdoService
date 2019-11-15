<?php

namespace RMysql;

class PdoService
{
    protected   static $_instance = null;
    protected   $dbName = '';
    protected   $dsn;
    protected   $dbh;
    protected   $connectMaxNums   = 3;
    protected   $logFile          = 'PdoService.log';
    protected   $connectNums      = 0;

    /**
     * 构造
     * @return
     */
     function __construct($dbHost, $dbUser, $dbPasswd, $dbName, $dbCharset='utf8mb4')
    {

            pdoConnect:
            if($this->connectNums>=$this->connectMaxNums){
                $this->outputError("重试次数:".$this->connectNums);
                return false;
            }
            try {
                $this->dsn = 'mysql:host=' . $dbHost . ';dbname=' . $dbName;
                # ATTR_TIMEOUT 连接超时时间
                # ATTR_PERSISTENT TRUE：是长连接（长连接的使用必须要配合：Apache（connection：keepAlive），Mysqld）,FALSE：默认的，非长连接
                $where = [
                    \PDO::MYSQL_ATTR_INIT_COMMAND => "set names ".$dbCharset,
                 #   \PDO::ATTR_TIMEOUT            => 3,
                 #   \PDO::ATTR_ERRMODE            => \PDO::ERRMODE_EXCEPTION
                 #    \PDO::ATTR_PERSISTENT         => true,
                ];

                $this->dbh = new \PDO($this->dsn, $dbUser, $dbPasswd,$where);
                #$this->dbh->exec('SET character_set_connection='.$dbCharset.', character_set_results='.$dbCharset.', character_set_client=binary');
                #$this->dbh->setAttribute(PDO::ATTR_TIMEOUT,3);
            } catch (PDOException $e) {
                $this->connectNums++;
                $this->outputError('PDOException'.$e->getMessage());
                goto pdoConnect;
            }
            $this->connectNums = 0;
    }

    /**
     * 防止克隆
     *
     */
    private function __clone() {}

    /**
     * Singleton instance
     * # $dbHost='', $dbUser='', $dbPasswd='', $dbName=''
     * @return Object
     */
    public static function getInstance()
    {
       # ini_set("default_socket_timeout", 180);
        $conn = 0;
        toInstance:
        if (self::$_instance === null) {
            $dbHost     = getenv('mysql_host');
            $dbUser     = getenv('mysql_username');
            $dbPasswd   = getenv('mysql_password');
            $dbName     = getenv('mysql_database');
            self::$_instance = new self($dbHost, $dbUser, $dbPasswd, $dbName);
        }
        try {
            $dbh   = self::$_instance->dbh;
            $check = $dbh->query("SELECT 1");
            if ( !$check ||  $dbh->errorCode() != '00000') {
              //  LogService::getLogger(['log_file' =>'PdoService.log'])->error(' errorCode no == 00000 :' . $dbh->errorCode().' conn:'.$conn);
                $arrayError = $dbh->errorInfo();
               // LogService::getLogger(['log_file' =>'PdoService.log'])->error(' errorCode instance:' . json_encode($arrayError));
                if($conn<=3) {
                    $conn++;
                    self::$_instance = null;
                    goto toInstance;
                }

            }
        }catch (Exception $e){
          //  LogService::getLogger(['log_file' =>'PdoService.log'])->error(' errorCode:' . $dbh->errorCode());
           // LogService::getLogger(['log_file' =>'PdoService.log'])->error(' instance:' . $e->getMessage());
            if($conn<=3) {
                $conn++;
                self::$_instance = null;
                goto toInstance;
            }
        }
      #  LogService::getLogger(['log_file' =>'PdoService.log'])->info(' end:' . json_encode(self::$_instance).'  conn:'.$conn);
        $conn = 0;
        return self::$_instance;
    }

    /**
     * 关闭重新链接
     * @param $dbHost
     * @param $dbUser
     * @param $dbPasswd
     * @param $dbName
     * @param $dbCharset
     * @return PdoService|null
     */
    public static function cloneInstance(){

        self::$_instance = null;
        echo date("Y-m-d H:i:s").":clone to :".json_encode( self::$_instance).PHP_EOL;
        return self::$_instance;
    }

    /**
     * Query 查询
     *
     * @param String $strSql SQL语句
     * @param String $queryMode 查询方式(All or Row)
     * @param Boolean $debug
     * @return Array
     */
    public function query($strSql, $queryMode = 'All', $debug = false)
    {
        if ($debug === true) $this->debug($strSql);
        $recordset = $this->dbh->query($strSql);
        $this->getPDOError();
        if ($recordset) {
            $recordset->setFetchMode(\PDO::FETCH_ASSOC);
            if ($queryMode == 'All') {
                $result = $recordset->fetchAll();
            } elseif ($queryMode == 'Row') {
                $result = $recordset->fetch();
            }
        } else {
            $result = null;
        }
        return $result;
    }

    /**
     * Update 更新
     *
     * @param String $table 表名
     * @param Array $arrayDataValue 字段与值
     * @param String $where 条件
     * @param Boolean $debug
     * @return Int
     */
    public function update($table, $arrayDataValue, $where = '', $debug = false)
    {
        $this->checkFields($table, $arrayDataValue);
        if ($where) {
            $strSql = '';
            foreach ($arrayDataValue as $key => $value) {
                $strSql .= ", `$key`='$value'";
            }
            $strSql = substr($strSql, 1);
            $strSql = "UPDATE `$table` SET $strSql WHERE $where";
        } else {
            $strSql = "REPLACE INTO `$table` (`".implode('`,`', array_keys($arrayDataValue))."`) VALUES ('".implode("','", $arrayDataValue)."')";
        }
        if ($debug === true) $this->debug($strSql);
        $result = $this->dbh->exec($strSql);
        $this->getPDOError();
        return $result;
    }

    /**
     * Insert 插入
     *
     * @param String $table 表名
     * @param Array $arrayDataValue 字段与值
     * @param Boolean $debug
     * @return Int
     */
    public function insert($table, $arrayDataValue, $debug = false)
    {
        $this->checkFields($table, $arrayDataValue);
        $strSql = "INSERT INTO `$table` (`".implode('`,`', array_keys($arrayDataValue))."`) VALUES ('".implode("','", $arrayDataValue)."')";
        if ($debug === true) $this->debug($strSql);
        $result = $this->dbh->exec($strSql);
        $this->getPDOError();
        return $result;
    }

    public function butchInsert($table, $arrayDataValue, $debug = false){

        $strSql = " INSERT INTO `$table` (`".implode('`,`', array_keys($arrayDataValue[0]))."`) VALUES  ";
        if($arrayDataValue){
            foreach ($arrayDataValue as $val){
                $strSql .= '(';
                $valData = array_keys($val);
                foreach ($valData as $key1=>$val1){
                    $strSql .= '"'.$val[$val1].'",';
                }
                $strSql = substr($strSql,0,strlen($strSql)-1);
                $strSql .= '),';
            }
            $strSql = substr($strSql,0,strlen($strSql)-1);
        }
        if ($debug === true) $this->debug($strSql);
        $result = $this->dbh->exec($strSql);
        $this->getPDOError();
        return $result;
    }

    /**
     * Replace 覆盖方式插入
     *
     * @param String $table 表名
     * @param Array $arrayDataValue 字段与值
     * @param Boolean $debug
     * @return Int
     */
    public function replace($table, $arrayDataValue, $debug = false)
    {
        $this->checkFields($table, $arrayDataValue);
        $strSql = "REPLACE INTO `$table`(`".implode('`,`', array_keys($arrayDataValue))."`) VALUES ('".implode("','", $arrayDataValue)."')";
        if ($debug === true) $this->debug($strSql);
        $result = $this->dbh->exec($strSql);
        $this->getPDOError();
        return $result;
    }

    /**
     * Delete 删除
     *
     * @param String $table 表名
     * @param String $where 条件
     * @param Boolean $debug
     * @return Int
     */
    public function delete($table, $where = '', $debug = false)
    {
        if ($where == '') {
            $this->outputError("'WHERE' is Null");
        } else {
            $strSql = "DELETE FROM `$table` WHERE $where";
            if ($debug === true) $this->debug($strSql);
            $result = $this->dbh->exec($strSql);
            $this->getPDOError();
            return $result;
        }
    }

    /**
     * execSql 执行SQL语句,debug=>true可打印sql调试
     *
     * @param String $strSql
     * @param Boolean $debug
     * @return Int
     */
    public function execSql($strSql, $debug = false)
    {
        if ($debug === true) $this->debug($strSql);
        $result = $this->dbh->exec($strSql);
        $this->getPDOError();
        return $result;
    }

    /**
     * 获取字段最大值
     *
     * @param string $table 表名
     * @param string $field_name 字段名
     * @param string $where 条件
     */
    public function getMaxValue($table, $field_name, $where = '', $debug = false)
    {
        $strSql = "SELECT MAX(".$field_name.") AS MAX_VALUE FROM $table";
        if ($where != '') $strSql .= " WHERE $where";
        if ($debug === true) $this->debug($strSql);
        $arrTemp = $this->query($strSql, 'Row');
        $maxValue = $arrTemp["MAX_VALUE"];
        if ($maxValue == "" || $maxValue == null) {
            $maxValue = 0;
        }
        return $maxValue;
    }

    /**
     * 获取指定列的数量
     *
     * @param string $table
     * @param string $field_name
     * @param string $where
     * @param bool $debug
     * @return int
     */
    public function getCount($table, $field_name, $where = '', $debug = false)
    {
        $strSql = "SELECT COUNT($field_name) AS NUM FROM $table";
        if ($where != '') $strSql .= " WHERE $where";
        if ($debug === true) $this->debug($strSql);
        $arrTemp = $this->query($strSql, 'Row');
        return $arrTemp['NUM'];
    }

    /**
     * 获取表引擎
     *
     * @param String $dbName 库名
     * @param String $tableName 表名
     * @param Boolean $debug
     * @return String
     */
    public function getTableEngine($dbName, $tableName)
    {
        $strSql = "SHOW TABLE STATUS FROM $dbName WHERE Name='".$tableName."'";
        $arrayTableInfo = $this->query($strSql);
        $this->getPDOError();
        return $arrayTableInfo[0]['Engine'];
    }
    //预处理执行
    public function prepareSql($sql=''){
        return $this->dbh->prepare($sql);
    }
    //执行预处理
    public function execute($presql){
        return $this->dbh->execute($presql);
    }

    /**
     * pdo属性设置
     */
    public function setAttribute($p,$d){
        $this->dbh->setAttribute($p,$d);
    }

    /**
     * beginTransaction 事务开始
     */
    public function beginTransaction()
    {
        $this->dbh->beginTransaction();
    }

    /**
     * commit 事务提交
     */
    public function commit()
    {
        $this->dbh->commit();
    }

    /**
     * rollback 事务回滚
     */
    public function rollback()
    {
        $this->dbh->rollback();
    }

    /**
     * transaction 通过事务处理多条SQL语句
     * 调用前需通过getTableEngine判断表引擎是否支持事务
     *
     * @param array $arraySql
     * @return Boolean
     */
    public function execTransaction($arraySql)
    {
        $retval = 1;
        $this->beginTransaction();
        foreach ($arraySql as $strSql) {
            if ($this->execSql($strSql) == 0) $retval = 0;
        }
        if ($retval == 0) {
            $this->rollback();
            return false;
        } else {
            $this->commit();
            return true;
        }
    }

    /**
     * checkFields 检查指定字段是否在指定数据表中存在
     *
     * @param String $table
     * @param array $arrayField
     */
    private function checkFields($table, $arrayFields)
    {
        $fields = $this->getFields($table);
        foreach ($arrayFields as $key => $value) {
            if (!in_array($key, $fields)) {
                $this->outputError("Unknown column `$key` in field list.");
            }
        }
    }

    /**
     * getFields 获取指定数据表中的全部字段名
     *
     * @param String $table 表名
     * @return array
     */
    private function getFields($table)
    {
        $fields = array();
        $recordset = $this->dbh->query("SHOW COLUMNS FROM $table");
        $this->getPDOError();
        $recordset->setFetchMode(\PDO::FETCH_ASSOC);
        $result = $recordset->fetchAll();
        foreach ($result as $rows) {
            $fields[] = $rows['Field'];
        }
        return $fields;
    }

    /**
     * getPDOError 捕获PDO错误信息
     */
    private function getPDOError()
    {
        if ($this->dbh->errorCode() != '00000') {
            $arrayError = $this->dbh->errorInfo();
            $this->outputError('pod error:'.json_encode($arrayError));
        }
    }

    /**
     * debug
     *
     * @param mixed $debuginfo
     */
    private function debug($debuginfo)
    {
        var_dump($debuginfo);
        exit();
    }

    /**
     * 输出错误信息
     *
     * @param String $strErrMsg
     */
    private function outputError($strErrMsg)
    {
        //throw new \Exception('MySQL Error: '.$strErrMsg);
      //  LogService::getLogger(['log_file' => $this->logFile])->error(' MySQL Error:' . $strErrMsg);

    }

    public function checkLoginStatus(){

    }


    /**
     * destruct 关闭数据库连接
     */
    public function destruct()
    {
        $this->dbh = null;
    }

    /**
     *PDO执行sql语句,返回改变的条数
     *如需调试可选用execSql($sql,true)
     * @param $sql
     * @return mixed
     */
    public function exec($sql=''){
        return $this->dbh->exec($sql);
    }

    public function lastinsertid(){
        return $this->dbh->lastinsertid();
    }

}
