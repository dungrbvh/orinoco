package com.orinoco.adapter.sessionize.sections.post

case class Duration(
                     time_stamp_epoch: Option[Int] = None,
                     uuid: Option[String] = None,
                     duration: Option[Int] = None
                   )

object Duration {
  val empty: Duration = Duration()
}