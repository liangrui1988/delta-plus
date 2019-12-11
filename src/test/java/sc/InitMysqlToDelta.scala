package sc

import org.apache.spark.sql.SparkSession

object InitMysqlToDelta {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      //      .config("hive.metastore.uris", "thrift://10.200.102.187:9083")
      .appName(this.getClass.getName)
      //      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      //      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val mysqlConf = Map(
      "url" -> "jdbc:mysql://10.200.102.197:3306/mbcj_test?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "candao",
      "dbtable" -> "script_file"
    )
    import org.apache.spark.sql.functions.col
    var df = spark.read.format("jdbc").options(mysqlConf).load()
    //.filter(c=>c.equals("script_file"))
    df = df.repartitionByRange(2, col("id"))
    df.show()
    df.write
      .format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
      mode("overwrite").
      save("/tmp/init_table")
  }

}
