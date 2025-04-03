package com.orinoco.adapter.postcalculate.cache

case class PrevDimensionHolder(
                                prevSessionID: Option[String] = None,
                                prevDomain1: Option[String] = None,
                                prevDomain2: Option[String] = None,
                                prevDomain3: Option[String] = None,
                                prevDomain4: Option[String] = None,
                                prevDomain5: Option[String] = None,
                                prevPath1: Option[String] = None,
                                prevPath2: Option[String] = None,
                                prevPath3: Option[String] = None,
                                prevPath4: Option[String] = None,
                                prevPath5: Option[String] = None,
                                prevPGN1: Option[String] = None,
                                prevPGN2: Option[String] = None,
                                prevPGN3: Option[String] = None,
                                prevPGN4: Option[String] = None,
                                prevPGN5: Option[String] = None,
                                prevSSC1: Option[String] = None,
                                prevSSC2: Option[String] = None,
                                prevSSC3: Option[String] = None,
                                prevSSC4: Option[String] = None,
                                prevSSC5: Option[String] = None
                              )

object PrevDimensionHolder {
  val empty: PrevDimensionHolder = PrevDimensionHolder()
}