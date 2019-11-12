package org.apache.spark.sql.delta.commands

import java.util.{Date, TimeZone, UUID}

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.delta.sources.{BFItem, FullOuterJoinRow}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession, functions => F}
import tech.mlsql.common.BloomFilter

import scala.collection.mutable.ArrayBuffer

case class UpsertTableInDelta(_data: Dataset[_],
                              saveMode: Option[SaveMode],
                              outputMode: Option[OutputMode],
                              deltaLog: DeltaLog,
                              options: DeltaOptions,
                              partitionColumns: Seq[String],
                              configuration: Map[String, String]
                             ) extends RunnableCommand
  with ImplicitMetadataOperation
  with DeltaCommand with DeltaCommandsFun {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val runId = UUID.randomUUID().toString
    assert(configuration.contains(UpsertTableInDelta.ID_COLS), "idCols is required ")

    if (outputMode.isDefined) {
      assert(outputMode.get == OutputMode.Append(), "append is required ")
    }

    if (saveMode.isDefined) {
      assert(saveMode.get == SaveMode.Append, "append is required ")
    }

    val upsertConf = new UpsertTableInDeltaConf(configuration, deltaLog, sparkSession)
    val upsertCommit = new UpsertCommit(deltaLog, runId, upsertConf)

    var actions = Seq[Action]()

    saveMode match {
      case Some(mode) =>
        deltaLog.withNewTransaction { txn =>
          txn.readWholeTable()
          if (!upsertConf.isPartialMerge) {
            updateMetadata(txn, _data, partitionColumns, configuration, false)
          }
          actions = upsert(txn, sparkSession, runId)
          val operation = DeltaOperations.Write(SaveMode.Overwrite,
            Option(partitionColumns),
            options.replaceWhere)
          upsertCommit.commit(txn, actions, operation)

        }
      case None => outputMode match {
        case Some(mode) =>
          val queryId = sparkSession.sparkContext.getLocalProperty(StreamExecution.QUERY_ID_KEY)
          assert(queryId != null)

          if (SchemaUtils.typeExistsRecursively(_data.schema)(_.isInstanceOf[NullType])) {
            throw DeltaErrors.streamWriteNullTypeException
          }

          val txn = deltaLog.startTransaction()
          txn.readWholeTable()
          // Streaming sinks can't blindly overwrite schema.
          // See Schema Management design doc for details
          if (!upsertConf.isPartialMerge) {
            updateMetadata(
              txn,
              _data,
              partitionColumns,
              configuration = Map.empty,
              false)
          }
          val currentVersion = txn.txnVersion(queryId)
          val batchId = configuration(UpsertTableInDelta.BATCH_ID).toLong
          if (currentVersion >= batchId) {
            logInfo(s"Skipping already complete epoch $batchId, in query $queryId")
          } else {
            actions = upsert(txn, sparkSession, runId)
            val setTxn = SetTransaction(queryId,
              batchId, Some(deltaLog.clock.getTimeMillis())) :: Nil
            val info = DeltaOperations.StreamingUpdate(outputMode.get, queryId, batchId)
            upsertCommit.commit(txn, setTxn ++ actions, info)
          }
      }
    }

    if (actions.size == 0) Seq[Row]() else {
      actions.map(f => Row.fromSeq(Seq(f.json)))
    }
  }

  def upsert(txn: OptimisticTransaction, sparkSession: SparkSession, runId: String): Seq[Action] = {

    val upsertConf = new UpsertTableInDeltaConf(configuration, deltaLog, sparkSession)
    val upsertBF = new UpsertBF(upsertConf, runId)
    val isDelete = upsertConf.isDeleteOp

    // if _data is stream dataframe, we should convert it to normal
    // dataframe and so we can join it later
    var data = convertStreamDataFrame(_data)


    import sparkSession.implicits._
    val snapshot = deltaLog.snapshot
    val metadata = deltaLog.snapshot.metadata

    /**
     * Firstly, we should get all partition columns from `idCols` condition.
     * Then we can use them to optimize file scan.
     */
    val idCols = configuration.getOrElse(UpsertTableInDelta.ID_COLS, "")
    val idColsList = idCols.split(",").filterNot(_.isEmpty).toSeq
    val partitionColumnsInIdCols = partitionColumns.intersect(idColsList)

    // we should make sure the data have no duplicate otherwise throw exception
    if (upsertConf.isDropDuplicate()) {
      data = data.dropDuplicates(idColsList.toArray)
    } else {
      val tempDF = data.groupBy(idColsList.map(col => F.col(col)): _*).agg(F.count("*").as("count"))
      if (tempDF.filter("count > 1").count() != 0) {
        throw new RuntimeException("Cannot perform MERGE as multiple source rows " +
          "matched and attempted to update the same target row in the Delta table.")
      }
    }

    val sourceSchema = if (upsertConf.isInitial) data.schema else snapshot.schema


    if (upsertConf.isInitial && upsertConf.isPartialMerge) {
      throw new RuntimeException(s"Please init the table or disable ${UpsertTableInDelta.PARTIAL_MERGE}")
    }

    if (upsertConf.isInitial) {

      deltaLog.fs.mkdirs(deltaLog.logPath)

      val newFiles = if (!isDelete) {
        txn.writeFiles(data.repartition(1), Some(options))
      } else Seq()
      upsertBF.generateBFForParquetFile(sourceSchema, newFiles, Seq())
      return newFiles
    }


    val partitionFilters = if (partitionColumnsInIdCols.size > 0) {
      val schema = data.schema

      def isNumber(column: String) = {
        schema.filter(f => f.name == column).head.dataType match {
          case _: LongType => true
          case _: IntegerType => true
          case _: ShortType => true
          case _: DoubleType => true
          case _ => false
        }
      }

      val minMaxColumns = partitionColumnsInIdCols.flatMap { column =>
        Seq(F.lit(column), F.min(column).as(s"${column}_min"), F.max(F.max(s"${column}_max")))
      }.toArray
      val minxMaxKeyValues = data.select(minMaxColumns: _*).collect()

      // build our where statement
      val whereStatement = minxMaxKeyValues.map { row =>
        val column = row.getString(0)
        val minValue = row.get(1).toString
        val maxValue = row.get(2).toString

        if (isNumber(column)) {
          s"${column} >= ${minValue} and   ${maxValue} >= ${column}"
        } else {
          s"""${column} >= "${minValue}" and   "${maxValue}" >= ${column}"""
        }
      }
      logInfo(s"whereStatement: ${whereStatement.mkString(" and ")}")
      val predicates = parsePartitionPredicates(sparkSession, whereStatement.mkString(" and "))
      Some(predicates)

    } else None


    val filterFilesDataSet = partitionFilters match {
      case None =>
        snapshot.allFiles
      case Some(predicates) =>
        DeltaLog.filterFileList(
          metadata.partitionColumns, snapshot.allFiles.toDF(), predicates).as[AddFile]
    }


    // Again, we collect all files to driver,
    // this may impact performance and even make the driver OOM when
    // the number of files are very huge.
    // So please make sure you have configured the partition columns or make compaction frequently

    val filterFiles = filterFilesDataSet.collect

    // filter files are affected by BF
    val bfPath = new Path(deltaLog.dataPath, "_bf_index_" + deltaLog.snapshot.version)
    val filesAreAffectedWithDeltaFormat = if (upsertConf.isBloomFilterEnable && deltaLog.fs.exists(bfPath)) {
      val schemaNames = data.schema.map(f => f.name)
      val dataBr = sparkSession.sparkContext.broadcast(data.collect())
      val affectedFilePaths = sparkSession.read.parquet(bfPath.toUri.getPath).as[BFItem].flatMap { bfItem =>
        val bf = new BloomFilter(bfItem.bf)
        var dataInBf = false
        dataBr.value.foreach { row =>
          if (!dataInBf) {
            val key = UpsertTableInDelta.getKey(row.asInstanceOf[Row], idColsList, schemaNames)
            dataInBf = bf.mightContain(key)
          }

        }
        if (dataInBf) List(bfItem.fileName) else List()
      }.as[String].collect()
      filterFiles.filter(f => affectedFilePaths.contains(f.path))
    } else {
      // filter files are affected by anti join
      val dataInTableWeShouldProcess = deltaLog.createDataFrame(snapshot, filterFiles, false)
      val dataInTableWeShouldProcessWithFileName = dataInTableWeShouldProcess.
        withColumn(UpsertTableInDelta.FILE_NAME, F.input_file_name())
      // get all files that are affected by the new data(update)
      val filesAreAffected = dataInTableWeShouldProcessWithFileName.join(data,
        usingColumns = idColsList,
        joinType = "inner").select(UpsertTableInDelta.FILE_NAME).
        distinct().collect().map(f => f.getString(0))

      val tmpFilePathSet = filesAreAffected.map(f => f.split("/").last).toSet

      filterFiles.filter { file =>
        tmpFilePathSet.contains(file.path.split("/").last)
      }
    }

    val deletedFiles = filesAreAffectedWithDeltaFormat.map(_.remove)

    // we should get  not changed records in affected files and write them back again
    val affectedRecords = deltaLog.createDataFrame(snapshot, filesAreAffectedWithDeltaFormat, false)

    if (upsertConf.isPartialMerge) {
      // new data format: {IDs... value:...}  value should be JSon/StructType,so we can merge it into table
      // the order of fields are important
      val newDF = affectedRecords.join(data,
        usingColumns = idColsList, joinType = "fullOuter")
      val sourceLen = sourceSchema.fields.length
      val sourceSchemaSeq = sourceSchema.map(f => f.name)
      val targetSchemaSeq = data.schema.map(f => f.name)
      val targetLen = data.schema.length

      val targetValueName = targetSchemaSeq.filterNot(name => idColsList.contains(name)).head
      val targetValueIndex = targetSchemaSeq.indexOf(targetValueName)
      val targetValueType = data.schema.filter(f => f.name == targetValueName).head.dataType
      val timeZone = data.sparkSession.sessionState.conf.sessionLocalTimeZone

      val newRDD = newDF.rdd.map { row =>
        //split row to two row
        val leftRow = Row.fromSeq((0 until sourceLen).map(row.get(_)))
        val rightRow = Row.fromSeq((sourceLen until (sourceLen + targetLen-idColsList.size)).map(row.get(_)))

        FullOuterJoinRow(leftRow, rightRow,
          !UpsertTableInDelta.isKeyAllNull(leftRow, idColsList, sourceSchemaSeq),
          !UpsertTableInDelta.isKeyAllNull(rightRow, idColsList, targetSchemaSeq))
      }.map { row: FullOuterJoinRow =>
        new UpsertMergeJsonToRow(row, sourceSchema, 0, timeZone).output
      }

      val newTempData = sparkSession.createDataFrame(newRDD, sourceSchema)
      val newFiles = if (!isDelete) {
        txn.writeFiles(newTempData, Some(options))
      } else Seq()

      if (upsertConf.isBloomFilterEnable) {
        upsertBF.generateBFForParquetFile(sourceSchema, newFiles, deletedFiles)
      }
      logInfo(s"Update info: newFiles:${newFiles.size}  deletedFiles:${deletedFiles.size}")
      newFiles ++ deletedFiles

    } else {
      var notChangedRecords = affectedRecords.join(data,
        usingColumns = idColsList, joinType = "leftanti").
        drop(F.col(UpsertTableInDelta.FILE_NAME))

      if (configuration.contains(UpsertTableInDelta.FILE_NUM)) {
        notChangedRecords = notChangedRecords.repartition(configuration(UpsertTableInDelta.FILE_NUM).toInt)
      } else {
        // since new data will generate 1 file, and we should make sure new files from old files decrease one.
        if (notChangedRecords.rdd.partitions.length >= filesAreAffectedWithDeltaFormat.length && filesAreAffectedWithDeltaFormat.length > 1) {
          notChangedRecords = notChangedRecords.repartition(filesAreAffectedWithDeltaFormat.length - 1)
        }
      }

      val notChangedRecordsNewFiles = txn.writeFiles(notChangedRecords, Some(options))

      val newFiles = if (!isDelete) {
        val newTempData = data.repartition(1)
        txn.writeFiles(newTempData, Some(options))
      } else Seq()

      if (upsertConf.isBloomFilterEnable) {
        upsertBF.generateBFForParquetFile(sourceSchema, notChangedRecordsNewFiles ++ newFiles, deletedFiles)
      }
      logInfo(s"Update info: newFiles:${newFiles.size} notChangedRecordsNewFiles:${notChangedRecordsNewFiles.size} deletedFiles:${deletedFiles.size}")
      notChangedRecordsNewFiles ++ newFiles ++ deletedFiles
    }


  }

  override protected val canMergeSchema: Boolean = false
  override protected val canOverwriteSchema: Boolean = false

}

object UpsertTableInDelta {
  val ID_COLS = "idCols"
  val BATCH_ID = "batchId"
  val FILE_NAME = "__fileName__"
  val OPERATION_TYPE = "operation"
  val OPERATION_TYPE_UPSERT = "upsert"
  val OPERATION_TYPE_DELETE = "delete"
  val DROP_DUPLICATE = "dropDuplicate"

  val PARTIAL_MERGE = "partialMerge"

  val FILE_NUM = "fileNum"
  val BLOOM_FILTER_ENABLE = "bloomFilterEnable"

  def getKey(row: Row, idColsList: Seq[String], schemaNames: Seq[String]) = {
    getColStrs(row, idColsList, schemaNames)
  }

  def isKeyAllNull(row: Row, idColsList: Seq[String], schemaNames: Seq[String]) = {
    idColsList.sorted.map(col => schemaNames.indexOf(col)).count { col =>
      row.get(col) == null
    } == idColsList.length
  }

  def getColStrs(row: Row, cols: Seq[String], schemaNames: Seq[String]) = {
    val item = cols.sorted.map(col => schemaNames.indexOf(col)).map { col =>
      row.get(col).toString
    }.mkString("_")
    item
  }
}

class UpsertTableInDeltaConf(configuration: Map[String, String], @transient val deltaLog: DeltaLog, @transient val sparkSession: SparkSession) {
  def isDropDuplicate() = {
    configuration.get(UpsertTableInDelta.DROP_DUPLICATE).map(_.toBoolean).getOrElse(false)
  }

  def isBloomFilterEnable = {
    configuration.getOrElse(UpsertTableInDelta.BLOOM_FILTER_ENABLE, "false").toBoolean
  }

  def isDeleteOp = {
    configuration
      .getOrElse(UpsertTableInDelta.OPERATION_TYPE,
        UpsertTableInDelta.OPERATION_TYPE_UPSERT) == UpsertTableInDelta.OPERATION_TYPE_DELETE
  }

  def isInitial = {
    val readVersion = deltaLog.snapshot.version
    val isInitial = readVersion < 0
    isInitial
  }

  def isPartialMerge = {
    configuration
      .getOrElse(UpsertTableInDelta.PARTIAL_MERGE,
        "false").toBoolean
  }

  def bfErrorRate = {
    configuration.getOrElse("bfErrorRate", "0.0001").toDouble
  }

  def idColsList = {
    val idCols = configuration.getOrElse(UpsertTableInDelta.ID_COLS, "")
    idCols.split(",").filterNot(_.isEmpty).toSeq
  }


}

class UpsertCommit(deltaLog: DeltaLog, runId: String, upserConf: UpsertTableInDeltaConf) {

  def commit(txn: OptimisticTransaction, actions: Seq[Action], op: DeltaOperations.Operation): Long = {
    val currentV = deltaLog.snapshot.version.toInt

    def cleanTmpBFIndex(v: Long) = {
      try {
        val newBFPathFs = new Path(deltaLog.dataPath, "_bf_index_" + v + "_" + runId)
        deltaLog.fs.delete(newBFPathFs, true)
      } catch {
        case e1: Exception =>
      }
    }

    def cleanOldBFIndex(v: Long) = {
      try {
        val newBFPathFs = new Path(deltaLog.dataPath, "_bf_index_" + v)
        deltaLog.fs.delete(newBFPathFs, true)
      } catch {
        case e1: Exception =>
      }
    }

    def mvBFIndex(v: Long) = {
      try {
        val newBFPathFs = new Path(deltaLog.dataPath, "_bf_index_" + v + "_" + runId)
        val targetPath = new Path(deltaLog.dataPath, "_bf_index_" + v)
        deltaLog.fs.rename(newBFPathFs, targetPath)
      } catch {
        case e1: Exception =>
      }
    }

    val newV = try {
      txn.commit(actions, op)
    } catch {
      case e: Exception =>
        if (upserConf.isBloomFilterEnable) {
          cleanTmpBFIndex(currentV + 1)
        }
        throw e
    }
    if (newV > -1) {
      if (upserConf.isBloomFilterEnable) {
        mvBFIndex(newV)
        cleanOldBFIndex(newV - 1)
      }
    }
    newV
  }
}

class UpsertBF(upsertConf: UpsertTableInDeltaConf, runId: String) {

  import upsertConf.sparkSession.implicits._

  def generateBFForParquetFile(sourceSchema: StructType, addFiles: Seq[AddFile], deletedFiles: Seq[RemoveFile]) = {
    val deltaLog = upsertConf.deltaLog
    val snapshot = deltaLog.snapshot
    val sparkSession = upsertConf.sparkSession
    val isInitial = upsertConf.isInitial


    val newBFPathFs = new Path(deltaLog.dataPath, "_bf_index_" + (deltaLog.snapshot.version.toInt + 1) + "_" + runId)
    val newBFPath = newBFPathFs.toUri.getPath

    val bfPathFs = new Path(deltaLog.dataPath, "_bf_index_" + deltaLog.snapshot.version)
    val bfPath = bfPathFs.toUri.getPath

    if (deltaLog.fs.exists(bfPathFs)) {
      deltaLog.fs.mkdirs(newBFPathFs)
      val deletePaths = deletedFiles.map(f => f.path).toSet
      sparkSession.read.parquet(bfPath).repartition(1).as[BFItem].
        filter { f =>
          !deletePaths.contains(f.fileName)
        }.write.mode(SaveMode.Append).parquet(newBFPath)
    }

    // There are 2 possible reasons that there is no _bf_index_[version] directory:
    // 1. No upsert operation happens before
    // 2. It fails to create _bf_index_[version] in previous upsert operation/version. For example, application crash happens
    //    between commit and rename.
    //
    // When there is no _bf_index_[version], the we will back to join to find the affected files, and then
    // create new BF file for current version and the version uncommitted yet.
    //
    var realAddFiles = addFiles
    if (!deltaLog.fs.exists(bfPathFs) && deltaLog.snapshot.version > -1) {
      realAddFiles ++= deltaLog.snapshot.allFiles.collect()
      realAddFiles = realAddFiles.filterNot(addfile => deletedFiles.map(_.path).contains(addfile.path))
    }

    val deltaPathPrefix = deltaLog.snapshot.deltaLog.dataPath.toUri.getPath

    def createDataFrame(
                         addFiles: Seq[AddFile],
                         isStreaming: Boolean = false,
                         actionTypeOpt: Option[String] = None): DataFrame = {
      val actionType = actionTypeOpt.getOrElse(if (isStreaming) "streaming" else "batch")
      val fileIndex = new TahoeBatchFileIndex(sparkSession, actionType, addFiles, deltaLog, deltaLog.dataPath, snapshot)
      val relation = HadoopFsRelation(
        fileIndex,
        partitionSchema = StructType(Array[StructField]()),
        dataSchema = sourceSchema,
        bucketSpec = None,
        deltaLog.snapshot.fileFormat,
        deltaLog.snapshot.metadata.format.options)(sparkSession)

      Dataset.ofRows(sparkSession, LogicalRelation(relation, isStreaming = isStreaming))
    }

    val df = if (!isInitial) {
      deltaLog.createDataFrame(snapshot, realAddFiles, false).withColumn(UpsertTableInDelta.FILE_NAME, F.input_file_name())
    } else {
      createDataFrame(realAddFiles, false).withColumn(UpsertTableInDelta.FILE_NAME, F.input_file_name())
    }
    val FILE_NAME = UpsertTableInDelta.FILE_NAME
    //    println(
    //      s"""
    //         |###  bf stat ###
    //         |fileNumber: ${realAddFiles.size}
    //         |realAddFiles: ${realAddFiles.map(f => f.path).toSeq}
    //         |deletedFiles: ${deletedFiles.map(f => f.path).toSeq}
    //         |mapPartitions: ${df.repartition(realAddFiles.size, F.col(FILE_NAME)).rdd.partitions.size}
    //         |""".stripMargin)

    val schemaNames = df.schema.map(f => f.name)
    val errorRate = upsertConf.bfErrorRate
    val idColsList = upsertConf.idColsList
    val dfSchema = df.schema.map(f => f.name)
    val fileWithIndex = realAddFiles.zipWithIndex.map { f => (f._1.path, f._2) }.toMap
    val fileNum = fileWithIndex.size
    val rdd = df.rdd.map { row =>
      (UpsertTableInDelta.getColStrs(row, Seq(FILE_NAME), dfSchema), row)
    }.partitionBy(new Partitioner() {
      override def numPartitions: Int = fileNum

      override def getPartition(key: Any): Int = fileWithIndex(StringUtils.splitByWholeSeparator(key.toString, deltaPathPrefix).last.stripPrefix("/"))
    }).map(f => f._2).mapPartitionsWithIndex { (index, iter) =>
      val buffer = new ArrayBuffer[String]()
      var fileName: String = null
      var numEntries = 0
      while (iter.hasNext) {
        val row = iter.next()
        if (fileName == null) {
          fileName = row.getAs[String](FILE_NAME)
        }
        numEntries += 1
        buffer += UpsertTableInDelta.getKey(row, idColsList, schemaNames)
      }
      if (numEntries > 0) {
        val bf = new BloomFilter(numEntries, errorRate)
        buffer.foreach { rowId =>
          bf.add(rowId)
        }
        //        println(
        //          s"""
        //             |### gen bf ###
        //             |index: ${index}
        //             |fileName: ${StringUtils.splitByWholeSeparator(fileName, deltaPathPrefix).last.stripPrefix("/")}
        //             |bf: ${bf.serializeToString()}
        //             |numEntries: ${numEntries}
        //             |errorRate: ${errorRate}
        //             |rowIds: ${buffer.toList}
        //             |""".stripMargin)
        List[BFItem](BFItem(StringUtils.splitByWholeSeparator(fileName, deltaPathPrefix).last.stripPrefix("/"), bf.serializeToString(), bf.size(), (bf.size() / 8d / 1024 / 1024) + "m")).iterator
      } else {
        //        println(
        //          s"""
        //             |### gen bf ###
        //             |index: ${index}
        //             |fileName:
        //             |bf:
        //             |numEntries: ${numEntries}
        //             |errorRate: ${errorRate}
        //             |rowIds: ${buffer.toList}
        //             |""".stripMargin)
        List[BFItem]().iterator
      }

    }.repartition(1).toDF().as[BFItem].write.mode(SaveMode.Append).parquet(newBFPath)
  }
}

class UpsertMergeJsonToRow(row: FullOuterJoinRow, schema: StructType, targetValueIndex: Int, defaultTimeZone: String) {
  val timeZone: TimeZone = DateTimeUtils.getTimeZone(defaultTimeZone)


  private def parseJson(jsonStr: String, callback: (String, Any) => Unit) = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    val obj = parse(jsonStr)
    obj.asInstanceOf[JObject].obj.foreach { f =>
      val dataType = schema.filter(field => field.name == f._1).head.dataType
      val value = f._2 match {
        //        case JArray(arr) =>
        //        case JObject(obj)=>
        case JBool(v) => v
        case JNull => null
        //        case JNothing =>
        case JDouble(v) => v
        case JInt(v) =>
          dataType match {
            case IntegerType =>
              v
          }
        case JLong(v) =>
          dataType match {
            case TimestampType =>
              new Date(v)
            case DateType =>
              new Date(v)
            case LongType =>
              v
          }
        case JString(v) => v

      }
      callback(f._1, value)
    }
  }

  private def merge(left: Row, right: Row) = {
    val tempRow = left.toSeq.toArray
    parseJson(right.getAs[String](targetValueIndex), (k, v) => {
      left.toSeq.zipWithIndex.map { wow =>
        val index = wow._2
        val value = wow._1
        tempRow(index) = if (schema.fieldIndex(k) == index) v else value
      }
    })
    Row.fromSeq(tempRow)
  }

  def output = {
    row match {
      case FullOuterJoinRow(left, right, true, true) =>
        // upsert
        merge(left, right)

      case FullOuterJoinRow(left, right, true, false) =>
        // no change
        left
      case FullOuterJoinRow(left, right, false, true) =>
        // append
        merge(left, right)
    }
  }
}





