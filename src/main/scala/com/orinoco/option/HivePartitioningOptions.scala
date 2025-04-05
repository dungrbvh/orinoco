package com.orinoco.option

import com.orinoco.commons.DateUtils
import org.joda.time.DateTime
import scopt.OptionParser

case class HivePartitioningOptions(
                                    hour: DateTime = DateTime.now(),
                                    tableType: String = "",
                                    outputProcBucketName: String = "",
                                    phase: String = ""
                                  )

object HivePartitioningOptions {
  def apply(args: Array[String]): HivePartitioningOptions =
    parser
      .parse(args, HivePartitioningOptions())
      .getOrElse(throw new RuntimeException("Failed to parse parameters."))

  def parser: OptionParser[HivePartitioningOptions] = new OptionParser[HivePartitioningOptions]("Parse") {
    head("Parse")

    opt[String]("execDateHourUTC")
      .required()
      .action((arg, option) => option.copy(hour = DateUtils.hourIsoFormatter.parseDateTime(arg)))
      .text("Hour in YYYY-MM-DDTHH.")

    opt[String]("tableType")
      .required()
      .action((arg, option) => option.copy(tableType = arg))
      .text("Either 'public' or 'private'.")

    opt[String]("outputProcBucketName")
      .required()
      .action((arg, option) => option.copy(outputProcBucketName = arg))
      .text("Bucket name.")

    opt[String]("phase")
      .required()
      .action((arg, option) => option.copy(phase = arg))
      .text("Type of phase.")
  }
}