package com.orinoco.commons

import com.orinoco.commons.DateUtils.DateParts
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

object PathUtils {
  lazy val logger = LoggerFactory.getLogger(this.getClass)

  /* Path Directory Listings */
  def splaySg(filePath: String, fs: FileSystem): List[String] = {
    fs.listStatus(new Path(filePath))
      .map(_.getPath.getName)
      .filter(_.startsWith("service_group"))
      .map(_.split("=").last)
      .toList
  }

  def returnRecentDT(filePath: String, fs: FileSystem): String = {
    val pathBase = filePath.split("dt=").head
    val recentFSDT = fs
      .listStatus(new Path(pathBase))
      .map(_.getPath.getName)
      .max

    val dtPath = pathBase + recentFSDT

    println(s"Retrieving most recent path $dtPath.")
    dtPath
  }

  /* Path Creation */
  def pathDate(pathBase: String, date: DateTime): String = pathBase.format(date.utcyyyy, date.utcMM, date.utcdd)

  def pathDateHour(pathBase: String, dateHour: DateTime): String = pathBase.format(s"${dateHour.utcyyyy}-${dateHour.utcMM}-${dateHour.utcdd}T${dateHour.utcHH}")

  def pathAccAid(pathBase: String, acc: String, aid: String, dateHour: DateTime): String =
    pathBase.format(acc, aid, dateHour.utcyyyy, dateHour.utcMM, dateHour.utcdd, dateHour.utcHH)

  def pathBucketDateHour(pathBase: String, processingBucket: String, dateHour: DateTime): String =
    pathBase.format(processingBucket, dateHour.utcyyyy, dateHour.utcMM, dateHour.utcdd, dateHour.utcHH)

  def pathBucketDate(pathBase: String, processingBucket: String, dateHour: String): String =
    pathBase.format(processingBucket, dateHour)

  def pathBucketDatePhase(pathBase: String, processingBucket: String, dateHour: String, phaseType: String): String =
    pathBase.format(processingBucket, dateHour, phaseType)

  def pathBucketDatePhaseTable(
                                pathBase: String,
                                processingBucket: String,
                                dateHour: String,
                                phaseType: String,
                                tableType: String
                              ): String =
    pathBase.format(processingBucket, dateHour, phaseType, tableType)

  def pathBasinBucketDatePhase(
                                pathBase: String, basinType: String, processingBucket: String, dateHour: String, phaseType: String
                              ): String =
    pathBase.format(basinType, processingBucket, dateHour, phaseType)

  def pathBaseDateExtension(pathBase: String, dateHour: DateTime): String =
    pathBase.format(dateHour.utcyyyy, dateHour.utcMM, dateHour.utcdd, dateHour.utcHH)

  def pathBaseDatePhaseExtension(pathBase: String, dateHour: DateTime,  phaseType: String): String =
    pathBase.format(dateHour.utcyyyy, dateHour.utcMM, dateHour.utcdd, dateHour.utcHH, phaseType)

  def pathBaseBucketDateExtension(pathBase: String, processingBucket: String, dateHour: DateTime): String =
    pathBase.format(processingBucket, dateHour.utcyyyy, dateHour.utcMM, dateHour.utcdd, dateHour.utcHH)

  def pathBaseBucketDatePhaseExtension(
                                        pathBase: String, processingBucket: String, dateHour: DateTime, phaseType: String
                                      ): String = pathBase.format(
    processingBucket, dateHour.utcyyyy, dateHour.utcMM, dateHour.utcdd, dateHour.utcHH, phaseType
  )

  def pathSGList(sGList: List[String], pathBase: String): List[String] =
    for {
      sG <- sGList
      sGPath = pathBase.replace("_service_group_", sG)
    } yield sGPath

  def pathTable(table: String, pathBase: String): String = pathBase.replace("_table_", table)

  // Returns list of paths with _service_group_ replaced with those found in snapshots
  def pathListSgDatePhaseExtension(
                                    hivePath: String,
                                    bucketServiceGroups: Array[String],
                                    hourDT: DateTime,
                                    phaseType: String
                                  ): Array[String] = {
    bucketServiceGroups.map(sg => hivePath.replace("_service_group_", sg)).map(
      path => pathBaseDatePhaseExtension(path, hourDT, phaseType)
    )
  }

  // Returns path of the main service group of processed bucket
  def pathSgDatePhaseExtension(
                                hivePath: String,
                                bucketServiceGroups: String,
                                hourDT: DateTime,
                                phaseType: String
                              ): String = {
    pathBaseDatePhaseExtension(
      hivePath.replace("_service_group_", bucketServiceGroups),
      hourDT,
      phaseType
    )
  }

  def pathTableBucketDatePhaseExtension(
                                         hivePath: String,
                                         tableType: String,
                                         processingBucket: String,
                                         hourDT: DateTime,
                                         phaseType: String
                                       ): String = {
    pathBaseBucketDatePhaseExtension(
      hivePath.replace("_table_type_", tableType),
      processingBucket,
      hourDT,
      phaseType
    )
  }

  // Returns list of raw data paths filtered by processing bucket
  def pathListRawFilteredByProcessingBucket(
                                             streamLandingPath: String,
                                             hourDT: DateTime,
                                             procBucketName: String,
                                             processingBucketList: List[(String, String)]
                                           )
                                           (implicit spark: SparkSession): List[String] = {
    if (streamLandingPath.contains("/acc=")) {
      // Raw data path partitioned by acc/aid: on-prem streaming source data
      FilterRawByProcessingBucketUtils(
        hourDT,
        streamLandingPath,
        procBucketName,
        processingBucketList
      ).inputPathList
    } else {
      // Raw data path partitioned by processing bucket: gcp streaming source data
      List(pathBucketDateHour(streamLandingPath, procBucketName, hourDT))
    }
  }


  /* Path Checking */
  def returnExistingPath(filePathArray: Array[String])(spark: SparkSession): Array[String] = {
    filePathArray.filter(
      filePath => {
        val path = new Path(filePath)
        println(s"Checking filePath: $path")
        val filePathExists = getFs(filePath)(spark).exists(path)
        if (filePathExists)
          println(s"File path $filePathExists exists.")
        else
          println(s"File path $filePathExists does not exists.")
        filePathExists
      }
    )
  }

  def checkPathExistence(filePath: String, fs: FileSystem): Unit = {
    try {
      println(s"Checking filePath: $filePath")
      val path = new Path(filePath)
      if (fs.globStatus(path).nonEmpty)
        println(s"Path $filePath exists.")
      else
        throw new Exception(s"Path '$filePath' does not exist!")
    } catch {
      case e: Exception =>
        println(s"Exception encountered on checkPathExistence(): ${ExceptionUtils.getStackTrace(e)}")
        throw e
    }
  }

  def checkNoPriorPathExistence(filePath: String, fs: FileSystem): Unit =
    if (fs.exists(new Path(filePath)))
      throw new Exception(s"Output path already exists: " + filePath)

  /* Path Deletion */
  def forcePathDeletion(force: Boolean, filePath: String, fs: FileSystem): Unit =
    if (force) {
      try {
        fs.delete(new Path(filePath), true)
        println(s"Deleted prior file path: $filePath")
      } catch {
        case e: Throwable => println(e)
      }
    }

  def getFs(path: String)(implicit spark: SparkSession): FileSystem = {
    FileSystem.get(new Path(path).toUri, spark.sparkContext.hadoopConfiguration)
  }

  def getFs(path: String, hadoopConfiguration: Configuration): FileSystem = {
    FileSystem.get(new Path(path).toUri, hadoopConfiguration)
  }

  def createHDFSFileWithContent(path: String, content: String)(implicit spark: SparkSession): Boolean = {
    try {
      import java.io.{BufferedWriter, OutputStreamWriter}

      if(content.length < 1024) {
        logger.info(s"Writing ${content} to ${path}")
      }

      val fs = getFs(path)
      val hdfsPath = new Path(path)

      if (fs.exists(hdfsPath)) {
        fs.delete(hdfsPath, true)
      }
      val out = fs.create(hdfsPath)
      val outputStreamWriter = new OutputStreamWriter(out, "UTF-8")
      val bufferedWriter = new BufferedWriter(outputStreamWriter)
      bufferedWriter.write(content)
      bufferedWriter.flush()
      bufferedWriter.close()
      outputStreamWriter.close()
      out.close()
      true
    } catch {
      case (e: Exception) => {
        logger.error(s"Error on creating and HDFS File ${path}", e)
        false
      }
    }
  }

  def slurpHDFSFileContent(path: String, hadoopConfiguration: Configuration = new Configuration()): Option[String] = {
    import java.io.{BufferedReader, InputStreamReader}

    val fs = getFs(path, hadoopConfiguration)
    val fsPath = new Path(path)

    if (!fs.exists(fsPath)) {
      logger.warn(s"File ${path} does not exists.")
      None
    } else {
      try {
        val in = fs.open(fsPath)
        val inputStreamReader = new InputStreamReader(in)
        val bufferedReader = new BufferedReader(inputStreamReader)
        val str = Stream.continually(bufferedReader.readLine()).takeWhile(_ != null).mkString("\n")
        bufferedReader.close()
        inputStreamReader.close()
        in.close()
        Some(str)
      }
      catch {
        case e: Exception => {
          logger.error(f"Fail to read file ${path}", e)
          throw e
        }
      }
    }
  }

}
