package sc

import org.apache.spark.sql.SparkSession

object ReadDeltaShow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").load("/tmp/init_table")
    //    val df = spark.read.format("delta").load("/tmp/init_table")
    //    val df = spark.read.format("delta").load("/tmp/delta-tablem")
    df.show()

  }

}
