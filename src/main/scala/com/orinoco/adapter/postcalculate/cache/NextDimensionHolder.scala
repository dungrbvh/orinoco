package com.orinoco.adapter.postcalculate.cache

case class NextDimensionHolder(
                                nextSessionID: Option[String] = None,
                                nextDomain1: Option[String] = None,
                                nextDomain2: Option[String] = None,
                                nextDomain3: Option[String] = None,
                                nextDomain4: Option[String] = None,
                                nextDomain5: Option[String] = None,
                                nextPath1: Option[String] = None,
                                nextPath2: Option[String] = None,
                                nextPath3: Option[String] = None,
                                nextPath4: Option[String] = None,
                                nextPath5: Option[String] = None,
                                nextPGN1: Option[String] = None,
                                nextPGN2: Option[String] = None,
                                nextPGN3: Option[String] = None,
                                nextPGN4: Option[String] = None,
                                nextPGN5: Option[String] = None,
                                nextSSC1: Option[String] = None,
                                nextSSC2: Option[String] = None,
                                nextSSC3: Option[String] = None,
                                nextSSC4: Option[String] = None,
                                nextSSC5: Option[String] = None
                              )

object NextDimensionHolder {
  val empty: NextDimensionHolder = NextDimensionHolder()
}