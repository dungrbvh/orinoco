package com.orinoco.option

import com.orinoco.commons.DateUtils
import org.joda.time.DateTime
import scopt.OptionParser

case class NormalizeOptions(
                           hour: DateTime = DateTime.now(),
                           outputBucketName: String = "",
                           partition_boost_factor: Int = 1,
                           force: Boolean = false
                           )

object NormalizeOptions {
  def apply(args: Array[String]): NormalizeOptions =
    parser
      .parse(args, NormalizeOptions())
      .getOrElse(throw new RuntimeException("Failed to parse parameters"))

  def parser: OptionParser[NormalizeOptions] = new OptionParser[NormalizeOptions]("Parse") {
    head("Parser")

    opt[String]("execDateHourUTC")
      .required()
      .action((arg, option) => option.copy(hour = DateUtils.hourIsoFormatter.parseDateTime(arg)))
      .text("Hour in YYYY-MM-DDTHH")

    opt[String]("outputBucketName")
      .required()
      .action((arg, option) => option.copy(outputBucketName = arg))
      .text("Bucket name")

    opt[Int]("partition_boost_factor")
      .optional()
      .action((arg, option) => option.copy(partition_boost_factor = arg.toInt))
      .text("Increase the number of output partitions to handle larger than expected hours.")

    opt[String]("force")
      .action((arg, option) => option.copy(force = arg.toBoolean))
      .text("Remove existing output before (re)running.")
  }
}