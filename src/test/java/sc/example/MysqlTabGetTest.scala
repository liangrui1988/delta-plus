package sc.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Test

/**
  * create by roy 2019/12/11   测试例子
  */
class MysqlTabGetTest {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName(this.getClass.getName)
    .getOrCreate()
  var mysqlConf = Map(
    "url" -> "jdbc:mysql://10.200.102.197:3306/mbcj_test?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
    "driver" -> "com.mysql.jdbc.Driver",
    "user" -> "root",
    "password" -> "candao"
  )

  @Test def initAllTabel(): Unit = {
    mysqlConf += ("dbtable" -> "information_schema.tables")
    val dbTables = spark.read.format("jdbc").options(mysqlConf).load().filter("table_schema = 'mbcj_test'").select("TABLE_NAME")
    dbTables.show()
    val tableNames = dbTables.collect().flatMap(_.toSeq)
    tableNames.foreach(tname => {
      println(s"$tname======start init table to delta")
      mysqlConf += ("dbtable" -> tname.toString)
      var df = spark.read.format("jdbc").options(mysqlConf).load()
      import org.apache.spark.sql.functions.col
      df = df.repartitionByRange(2, col("id"))
      df.show()
      df.write
        .format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
        mode("overwrite").
        save(s"/tmp/mbcj_test/$tname")
    })
  }

  import org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource
  @Test def streamMysqlToDelta(): Unit = {
    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
      option("host", "10.200.102.197").
      option("port", "3306").
      option("userName", "root").
      option("password", "candao").
      option("bingLogNamePrefix", "mysql-bin").
      option("databaseNamePattern", "mbcj_test").
      option("tableNamePattern", "stest,script_file").
      option("tableNamePatternType", "stringList").
      option("binlogIndex", "1").
      option("binlogFileOf fset", "46418").
      load()
    val query = df.writeStream.
      outputMode(OutputMode.Append()).
      format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
      option("__path__", "/tmp/{db}/{table}").
      option("mode", "Overwrite"). //Append overred
      option("idCols", "id").
      option("duration", "5").
      option("syncType", "binlog").
      option("checkpointLocation", "/tmp/cpl-binlog3").
      option("path", "{db}/{table}").
      start()
    query.awaitTermination()

  }
  @Test def readTable(): Unit = {
    val df = spark.read.format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").load("/tmp/mbcj_test/script_file")
    df.show()
  }


  @Test def readTable2(): Unit = {
    val df = spark.read.format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").load("/tmp/mbcj_test/stest")
    df.show()
  }

  @Test def binlogConsoleTest(): Unit = {
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
    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .option("mode", "Append")
      .format("console")
      .option("checkpointLocation", "/tmp/binlogConsoleTest")
      .start()
    query.awaitTermination()
  }

}
