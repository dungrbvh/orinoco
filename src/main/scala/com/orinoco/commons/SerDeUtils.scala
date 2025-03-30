package com.orinoco.commons

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.reflect.ClassTag

object SerDeUtils {
  /**
   * Serialize a certain value
   *
   * @param v some value
   * @return serialized value in bytearray format
   * */
  def serialize(v: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(v)
    oos.close()
    stream.toByteArray
  }

  /**
   * Deserialize and return a serialized value to it's original format
   *
   * @param b serialized value
   * @return original or deserialized value
   * */
  def deserialize[T](b: Array[Byte])(implicit classTag: ClassTag[T]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(b))
    val value = ois.readObject()
    ois.close()
    value match {
      case v:T => v
      case other =>
        throw new Exception(
          s"Error Deserializing. Expecting '${classTag.toString()}' " +
            s"and got '$other' which is a '${other.getClass}'")
    }
  }

  def deserializeToOptional[T: ClassTag](value: Any): Option[T] = {
    value match {
      case Some(v: Array[Byte]) => Option(SerDeUtils.deserialize[T](v))
      case Some(v: T) => Option(v)
      case _ => None
    }
  }
}