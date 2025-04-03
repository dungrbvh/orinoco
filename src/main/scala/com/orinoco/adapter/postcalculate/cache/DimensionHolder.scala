package com.orinoco.adapter.postcalculate.cache

case class DimensionHolder(
                            prior: Option[PrevDimensionHolder] = None,
                            subsequent: Option[NextDimensionHolder] = None
                          )

object DimensionHolder {
  val empty: DimensionHolder = DimensionHolder()
}