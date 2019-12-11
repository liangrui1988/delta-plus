package sc

import java.util.regex.Pattern

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.MLSQLMultiDeltaOptions
import org.junit.Test

import scala.util.matching.Regex

class MysqlGetTables {


  @Test def getDBTables(): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()
    val mysqlConf = Map(
      "url" -> "jdbc:mysql://10.200.102.197:3306/mbcj_test?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "candao",
      "dbtable" -> "information_schema.tables"
    )
    val df = spark.read.format("jdbc").options(mysqlConf).load().filter("table_schema = 'mbcj_test'").select("TABLE_NAME")
    df.show()
    //    val tnames = df.collect().map(_.getAs[String](0)).toList
    val tnames = df.collect().flatMap(_.toSeq).toList
    println(tnames)
  }

  private var tableNamePattern: Option[Pattern] = None
  private var tableNamePatternRes: Option[Pattern] = None

  @Test def optionTest(): Unit = {
    val parameters = Map("tableNamePattern" -> "tab_name,tab_name_1,tab_name_2")
    val tableNamePattern = parameters.get("tableNamePattern")
    val tableNamePatternRes = tableNamePattern.map(Pattern.compile)
    //.flatMap(_.split(",").toSeq)
    println(tableNamePatternRes)
    val isok = tableNamePatternRes.map(_.matcher("tab_name_1").matches())
      .getOrElse(false)
    println(tableNamePattern.get)
    val iskk = tableNamePattern.get.split(",").contains("tab_name")
    println(iskk)
  }

  @Test def tmpTest(): Unit = {
    val options: Map[String, String] = Map("__path__" -> "{db}/{table}")
    var path = options(MLSQLMultiDeltaOptions.FULL_PATH_KEY)
    println(path)
    val tablePath = path.replace("{db}", "dbname").replace("{table}", "tablename")
    println(tablePath)
  }
}
