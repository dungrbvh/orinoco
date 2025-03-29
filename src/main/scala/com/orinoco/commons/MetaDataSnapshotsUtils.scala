package com.orinoco.commons

import org.joda.time.DateTime
import com.rakuten.rat.commons.PathUtils.pathDateHour
import com.sun.net.httpserver.Authenticator.Success
import org.apache.spark.sql.SparkSession

import scala.util.Failure

object MetaDataSnapshotsUtils {
  /**
   * Read values from a text file with Spark
   *
   * @param filePath File path on file system
   * @return List of lines in the file as String
   * */
  def fileToList(filePath: String)(implicit spark: SparkSession): List[String] = {
    spark
      .read
      .textFile(filePath)
      .collect
      .toList
  }

  /**
   * Parse 2 first columns from a tsv file lines to tuples
   *
   * @param linesList List of lines with values separated by '/t'
   * @return List of tuples with first and second value from each input line
   * */
  def fromTsvLinesToIterString(lineList: List[String]): List[(String, String)] = {
    linesList
      .flatMap {
        line =>
          val s = line.split("\t")
          Try((s(0), s(1))) match {
            case Success(value) => Some(value)
            case Failure(_) =>
              println(s"File had a bad format at line '$line'")
              None
          }
      }
  }

  /**
   * Read values from an hourly snapshot to String list
   *
   * @param snapshotPath Snapshot path to be completed with input hour
   * @param hourDT Input hour to read snapshot from
   * @return List of lines of snapshot file as String
   * */
  def getListFromHourlySnapshot(snapshotPath: String, hourDT: DateTime)(implicit spark: SparkSession): List[String] = {
    fileToList(pathDateHour(snapshotPath, hourDT))
  }

  /**
   * Read values from an hourly snapshot in tsv format to Tuple list
   *
   * @param snapshotPath Snapshot path to be completed with input hour
   * @param hourDT   Input hour to read snapshot from
   * @return List of tuples with first and second value from each line of snapshot file
   * */
  def getTuplesFromHourlyTsvSnapshot(snapshotPath: String, hourDT: DateTime)(implicit spark: SparkSession): List[(String, String)] = {
    fromTsvLinesToIterString(getListFromHourlySnapshot(snapshotPath, hourDT))
  }

  /**
   * Parse list of String tuples to list of (Int, String)
   *
   * @param iter List of String tuples with first values parsable to Int
   * @return List of parsed tuples
   * */
  def toMapKeyInt(iter: List[(String, String)]): Map[Int, String] =
    iter.map(p => (p._1.toInt, p._2)).toMap

  /**
   * Map timezone from hive table name to each service_group
   * Assume 1 service_group can have multiple databases but only 1 timezone
   *
   * @param list List of (service_group, hive_name)
   * @return List of service_group => timezone
   * */
  def timezoneFromTableNameMap(list: List[(String, String)]): Map[String, String] = {
    list.map {
      case (serviceGroup: String, hiveName: String) =>
        val timezone = hiveName.split("_").last
        serviceGroup -> timezone
    }.toMap
  }
}
