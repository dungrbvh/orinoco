package com.orinoco.application.hivePartitioning.steps

import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.commons.DateUtils.{HHFormatter, MMFormatter, ddFormatter, yyyyFormatter}
import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import com.orinoco.schema.hivePartitioning.PublicRatSessionized
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.slf4j.Logger

object ReadFlattenedRecords {
  /**
   * Objective:
   *    This script has three main purposes:
   *      1. Return a list of select flattened fields for a specified hive table
   *      2. Read the payload of the selected fields
   *      3. Filter payload based on input date hour of spark job
   *
   * @param phaseType                                     Specified phase type
   * @param tableType                                     Specified hive table
   * @param inputPath                                     Pathing of output
   * @param hourDT                                        Date hour to process
   * @param serviceGroupExclusionFor2ndUpdateList         Provides list of service groups to exclude
   * @param spark                                         Necessary for spark usage
   **/

  def apply(
             logger: Logger,
             phaseType: String,
             tableType: String,
             inputPath: String,
             serviceGroupExclusionFor2ndUpdateList: List[String],
             hourDT: DateTime
           )(implicit spark: SparkSession): DataFrame = {
    val filteredFieldListCut = getFlattenedFieldsList(logger, tableType)
    val records = readRecordsFlattened(phaseType, tableType, inputPath, serviceGroupExclusionFor2ndUpdateList, filteredFieldListCut)
    filterRecordsDatePartition(records, hourDT)
  }

  // Selects all fields, nested or not, to be used in SQL pull of payload
  object getFlattenedFieldsList {
    def apply(
             logger: Logger,
             tableType: String
             ): List[String] = {

      // Get flatten fields list
      val sqlFieldSelect = """\.(?:.(?!\.))+$""".r
      val fieldListCut = SessionizePayload.nestedFieldList.map {
        x =>
          val field = sqlFieldSelect.findAllIn(x).mkString
          val filePattern =
            if (field.nonEmpty) field.mkString
            else "." + x
          (x, fieldPattern)
      }
        .groupBy(_._2)
        .values
        .flatMap(_.headOption)
        .map(_._1)
        .toList

      // Filter and other fields
      val fieldsMap = fieldListCut.map(x => (x.split('.').last)).toMap
      val filteredFieldListCut =
        if (tableType == "public")
          PublicRatSessionized.nestedFieldList.map(fieldsMap)
        else if (tableType == "private")
          SessionizePayload.getFlattenedSchema.map(_.name).toList.map(fieldsMap)
        else
          throw new Exception("Table type must be 'public' or 'private'")
      logger.info(s"SQL Select for Field Names:\n\t" + filteredFieldListCut.mkString("\n\t"))
      filteredFieldListCut
    }
  }

  // Pulls all necessary fields via SQL
  object readRecordsFlattened {
    def apply(
               phaseType: String,
               tableType: String,
               inputPath: String,
               serviceGroupExclusionFor2ndUpdateList: List[String],
               filteredFieldListCut: List[String]
             )(implicit spark: SparkSession): DataFrame = {

      import spark.implicits._
      val df = spark
        .read
        .schema(getSchemaAsStruct[SessionizePayload])
        .parquet(inputPath)

      val filteredDF =
        if (phaseType == "traffic" && tableType == "private") // Exclude list of service groups not specified
          df.filter($"service_group".isin(serviceGroupExclusionFor2ndUpdateList: _*))
        else df

      filteredDf.selectExpr(filteredFieldListCut: _*) // Reads payload based on specified fields
    }
  }

  // Filters out data based on input date hour of spark job
  object filterRecordsDatePartition {
    def apply(records: DataFrame, hourDT: DataFrame): DataFrame = {
      records.filter(
        records("year") === yyyyFormatter.print(hourDT) &&
          records("month") === MMFormatter.print(hourDT) &&
          records("day") === ddFormatter.print(hourDT) &&
          records("hour") === HHFormatter.print(hourDT)
      )
    }
  }
}