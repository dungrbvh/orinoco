package com.orinoco.adapter.sessionize.cache.evar

import com.orinoco.commons.EvarSections._
import com.orinoco.schema.cdna.CustomerDNA

object EvarMapping {
  /**
   * Objective:
   *    This script has one main purpose:
   *      1. Applies base values to evarized fields via mapping.
   **/

  def toEvarSection(evarized: Map[String, Option[Any]], cdna: CustomerDNA): EvarSection = EvarSection(
    first = evarFirstGrabber(evarized, cdna),
    last = evarLastGrabber(evarized, cdna),
    last_ex_checkout = evarLastExCheckoutGrabber(evarized, cdna),
    cdna_first = cdnaFirstGrabber(cdna),
    cdna_last = cdnaLastGrabber(cdna),
    cdna_last_ex_checkout = cdnaLastExCheckoutGrabber(cdna),
    nd_first = ndFirstGrabber(evarized),
    nd_last = ndLastGrabber(evarized),
    nd_last_ex_checkout = ndLastExCheckoutGrabber(evarized)
  )
}