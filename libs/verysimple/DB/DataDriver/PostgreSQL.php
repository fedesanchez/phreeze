<?php 
/** @package verysimple::DB::DataDriver */

require_once("IDataDriver.php");
require_once("verysimple/DB/ISqlFunction.php");
require_once("verysimple/DB/DatabaseException.php");
require_once("verysimple/DB/DatabaseConfig.php");

/**
 * An implementation of IDataDriver that communicates with
 * a MySQL server.  This is one of the native drivers
 * supported by Phreeze
 *
 * @package    verysimple::DB::DataDriver
 * @author     VerySimple Inc. <noreply@verysimple.com>
 * @copyright  1997-2010 VerySimple Inc.
 * @license    http://www.gnu.org/licenses/lgpl.html  LGPL
 * @version    1.0
 */
class DataDriverPostgreSQL implements IDataDriver
{	
	/** @var characters that will be escaped */
	static $BAD_CHARS = array("\\","\0","\n","\r","\x1a","'",'"');
	
	/** @var characters that will be used to replace bad chars */
	static $GOOD_CHARS = array("\\\\","\\0","\\n","\\r","\Z","\'",'\"');
	
	/**
	 * @inheritdocs
	 */
	function GetServerType()
	{
		return "PostgreSQL";
	}
	
	function Ping($connection)
	{
		 return pg_ping($connection);
	}
	
	/**
	 * @inheritdocs
	 */
	function Open($connectionstring,$database,$username,$password,$charset='',$bootstrap='') 
	{
		if (!function_exists("pg_connect")) throw new DatabaseException('pgsql extension is not enabled on this server.',DatabaseException::$CONNECTION_ERROR);
		
		// if the port is provided in the connection string then strip it out and provide it as a separate param
		$hostAndPort = explode(":",$connectionstring);
		$host = $hostAndPort[0];
		$port = count($hostAndPort) > 1 ? $hostAndPort[1] : null;
		
        $pgString="host=$host user=$username password=$password dbname=$database port=$port";
           
		$connection = pg_connect($pgString);
		
		if ( !$connection )
		{
			throw new DatabaseException("Error connecting to database: " . $database,DatabaseException::$CONNECTION_ERROR);
		}
		
		if ($charset)
		{
			pg_set_client_encoding($connection,$charset);
			/*
			if ( mysqli_connect_errno() )
			{
				throw new DatabaseException("Unable to set charset: " . mysqli_connect_error(),DatabaseException::$CONNECTION_ERROR);
			}*/
		}
		
		if ($bootstrap)
		{
			$statements = explode(';',$bootstrap);
			foreach ($statements as $sql)
			{
				try 
				{
					$this->Execute($connection, $sql);
				}
				catch (Exception $ex)
				{
					throw new DatabaseException("problem with bootstrap sql: " . $ex->getMessage(),DatabaseException::$ERROR_IN_QUERY);
				}
			}
		}
		
		return $connection;
	}
	
	/**
	 * @inheritdocs
	 */
	function Close($connection) 
	{
		@pg_close($connection); // ignore warnings
	}
	
	/**
	 * @inheritdocs
	 */
	function Query($connection,$sql) 
	{        
        $sql=str_replace("`","",$sql);
        error_log($sql);
		if ( !$rs = @pg_query($connection,$sql) )
		{
			throw new DatabaseException(pg_last_error($connection),DatabaseException::$ERROR_IN_QUERY);
		}
		
		return $rs;
	}

	/**
	 * @inheritdocs
	 */
	function Execute($connection,$sql)         
	{
        
		if ( !$result = @pg_query($connection,$sql) )
		{
			throw new DatabaseException(pg_last_error($connection),DatabaseException::$ERROR_IN_QUERY);
		}
		
		return pg_affected_rows($result); // results instead of connection?
	}
	
	/**
	 * @inheritdocs
	 */
	function Fetch($connection,$rs) 
	{
		return pg_fetch_assoc($rs);
	}

	/**
	 * @inheritdocs
	 */
	function GetLastInsertId($connection) 
	{
		return (mysqli_insert_id($connection));
	}

	/**
	 * @inheritdocs
	 */
	function GetLastError($connection)
	{
		return pg_last_error($connection);
	}
	
	/**
	 * @inheritdocs
	 */
	function Release($connection,$rs) 
	{
		pg_free_result($rs);	
	}
	
	/**
	 * @inheritdocs
	 * this method currently uses replacement and not mysqli_real_escape_string
	 * so that a database connection is not necessary in order to escape.
	 * this way cached queries can be used without connecting to the DB server
	 */
	function Escape($val) 
	{
        error_log("entro a escape");
		return str_replace(self::$BAD_CHARS, self::$GOOD_CHARS, $val);
		// return mysqli_real_escape_string($val);
 	}

 	/**
 	 * @inheritdocs
 	 */
 	public function GetQuotedSql($val)
 	{
        error_log($val);
 		if ($val === null) return DatabaseConfig::$CONVERT_NULL_TO_EMPTYSTRING ? "''" : 'NULL';
 	
 		if ($val instanceof ISqlFunction) return $val->GetQuotedSql($this);
 	    
        $out="'" . $this->Escape($val) . "'";
        error_log($out); 		
        return $out;
 	}
 	
	/**
	 * @inheritdocs
	 */
 	function GetTableNames($connection, $dbname, $ommitEmptyTables = false) 
	{
        error_log("entro a tablename");
		$sql = "SELECT tablename FROM pg_tables where schemaname='public'";
		$rs = $this->Query($connection,$sql);
		
		$tables = array();
		/*
		while ( $row = $this->Fetch($connection,$rs) )
		{
			if ( $ommitEmptyTables == false || $rs['Data_free'] > 0 )
			{
				$tables[] = $row['Name'];
			}
		}
		*/
		return $tables;
 	}
	
	/**
	 * @inheritdocs
	 */
 	function Optimize($connection,$table) 
	{
		$result = "REINDEX TABLE $table";
		$rs = $this->Query($connection,"REINDEX TABLE ". $this->Escape($table));

		/*while ( $row = $this->Fetch($connection,$rs) )
		{
			$tbl = $row['Table'];
			if (!isset($results[$tbl])) $results[$tbl] = "";
			$result .= trim($results[$tbl] . " " . $row['Msg_type'] . "=\"" . $row['Msg_text'] . "\"");	
		}
		*/
		return $result;
	}

	/**
	 * @inheritdocs
	 */
	function StartTransaction($connection)
	{
		$this->Execute($connection, "SET AUTOCOMMIT=0");
		$this->Execute($connection, "START TRANSACTION");
	}
	
	/**
	 * @inheritdocs
	 */
	function CommitTransaction($connection)
	{
		$this->Execute($connection, "COMMIT");
		$this->Execute($connection, "SET AUTOCOMMIT=1");
	}
	
	/**
	 * @inheritdocs
	 */
	function RollbackTransaction($connection)
	{
		$this->Execute($connection, "ROLLBACK");
		$this->Execute($connection, "SET AUTOCOMMIT=1");
	}
	
}

?>
