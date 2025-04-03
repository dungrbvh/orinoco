package com.orinoco.option

import com.rakuten.rat.commons.DateUtils
import org.joda.time.DateTime
import scopt.OptionParser

case class SessionizeOptions(
                              hour: DateTime = DateTime.now(),
                              outputProcBucketName: String = "",
                              force: Boolean = false,
                              skipInputCache: Boolean = false
                            )

object SessionizeOptions {
  def apply(args: Array[String]): SessionizeOptions =
    parser
      .parse(args, SessionizeOptions)
      .getOrElse(throw new RuntimeException("Failed to parse parameters."))

  def parser: OptionParser[SessionizeOptions] = new OptionParser[SessionizeOptions]("Sessionize") {
    head("Sessionize")

    opt[String]("execDateHourUTC")
      .required()
      .action((arg, option) => option.copy(hour = DateUtils.hourIsoFormatter.parseDateTime(arg)))
      .text("Hour in YYYY-MM-DDTHH.")

    opt[String]("outputProcBucketName")
      .required()
      .action((arg, option) => option.copy(outputProcBucketName = arg))
      .text("Bucket name.")

    opt[String]("force")
      .action((arg, option) => option.copy(force = arg.toBoolean))
      .text("Remove existing output before (re)running.")

    opt[String]("skipInputCache")
      .action((arg, option) => option.copy(skipInputCache = arg.toBoolean))
      .text("If this is run the first time or testing, enable this config")
  }
}