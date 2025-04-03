package com.orinoco.commons

import scala.annotation.tailrec

object DebugUtils {
  @tailrec
  def checkCorruptedParquet(throwable: Option[Throwable]): Option[String] = {
    // IOException covers broad range of cases,
    // so we need to check the corrupted parquet case from the message too
    val ioExceptionPattern = "Expected \\d+ values in column chunk at \\S+\\.parquet offset \\d+ " +
      "but got \\d+ values instead over \\d+ pages ending at file offset \\d+"

    throwable match {
      case Some(e: org.apache.parquet.io.ParquetDecodingException) => Some(e.getMessage)
      case Some(e: java.io.IOException) if e.getMessage.matches(ioExceptionPattern) => Some(e.getMessage)
      case Some(e) => checkCorruptedParquet(Option(e.getCause))
      case None => None
    }
  }
}