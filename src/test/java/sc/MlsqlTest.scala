package sc


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object MlsqlTest {
  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .master("local[*]")
      //      .config("hive.metastore.uris", "thrift://10.200.102.187:9083")
      .appName(this.getClass.getName)
      //      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      //      .enableHiveSupport()
      .getOrCreate()
    //  import  org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource.DefaultSource
    //mysql-bin.000001
    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
      option("host", "10.200.102.197").
      option("port", "3306").
      option("userName", "root").
      option("password", "candao").
      option("bingLogNamePrefix", "mysql-bin").
      option("databaseNamePattern", "mbcj_test").
      option("tableNamePattern", "script_file").
      option("binlogIndex", "1").
      option("binlogFileOffset", "29313").
      load()

    import org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource
    val query = df.writeStream.
      outputMode(OutputMode.Append()).
      format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
      option("__path__", "/tmp/init_table").
      option("mode", "Overwrite"). //Append overred
      option("idCols", "id").
      option("duration", "5").
      option("syncType", "binlog").
      option("checkpointLocation", "/tmp/cpl-binlog2").
      option("path", "{db}/{table}").
      start()

    //    val query = df.writeStream
    //      .outputMode(OutputMode.Append)
    //      .option("mode","Append")
    //      .format("console")
    //      .option("checkpointLocation", "/tmp/cpl-binlog2")
    //      .start()

    query.awaitTermination()

  }

}
