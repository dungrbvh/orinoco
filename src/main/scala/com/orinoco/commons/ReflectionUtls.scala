package com.orinoco.commons

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import scala.reflect.runtime.universe._

object ReflectionUtls {
  def getSchemaAsStruct[T: TypeTag]: StrucType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
}