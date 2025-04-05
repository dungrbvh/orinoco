package com.orinoco.commons

import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.application.sessionize.post.evar._
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

object EvarUtils {

  /**
   * evarizeSessionizedPayload and evarizePostCalculatedPayload are constricted functions.
   * Each applies base values to evarized fields via mapping.
   *
   * @param recs            Session of user
   * @param evarStruct      Redefined evarization schema
   * @param evarDimensions  List of dimensions to be evarized
   * @return
   **/
  def evarizeSessionizedPayload(
                               recs: Seq[SessionizePayload], evarStruct: StructType, evarDimensions: List[String]
                               ): Map[String, Map[String, Option[Any]]] = {
   evarize(
     sessionizedRecordsToEvarizable(recs, evarStruct),
     evarDimensions
   )
  }

  def evarizePostCalculatedPayload(
                                    recs: Seq[SessionizePayload], evarStruct: StructType, evarDimensions: List[String]
                                  ): Map[String, Map[String, Option[Any]]] = {
    evarize(
      postCalculatedRecordsToEvarizable(recs, evarStruct),
      evarDimensions
    )
  }

  /**
   * @param records sessionized records
   * @param evarDimensions list of dimension to be evarized
   * @return
   **/
  def evarize(records: Seq[Evarizing], evarDimensions: List[String]): Map[String, Map[String, Option[Any]]] = {
    records
      .sortBy(x => (x.ts, x.uuid))
      .foldLeft(List.empty[(String, Map[String, Option[Any]])]){
        (r:List[(String, Map[String, Option[Any]])], c: Evarizing) =>
          val map = evarDimensions.flatMap {
            dim =>
              val firstEvarFieldName = s"$dim$FirstEvarFieldSuffix"
              val lastEvarFieldName = s"$dim$LastEvarFieldSuffix"
              val lastExCheckoutFieldName = s"$dim$LastExCheckoutEvarFieldSuffix"
              val isCheckout = c.dims.contains(PageTypeFieldName) && checkoutFinder(c.dims(PageTypeFieldName))

              val output =
                if (r.isEmpty) { // Checks first record

                  // refer from cache
                  val cacheFirst = c.cache.getOrElse(firstEvarFieldName, None)
                  val cacheLast = c.cache.getOrElse(lastEvarFieldName, None)
                  val cacheLastExCheckout = c.cache.getOrElse(lastExCheckoutFieldName, None)

                  val value = cleanIfMap(dim, c.dims.getOrElse(dim, None))

                  List(
                    firstEvarFieldName -> {
                      if(cacheFirst.isDefined) cacheFirst else value
                    }, // null (None)
                    lastEvarFieldName -> {
                      if(value.isEmpty) // if value is null, get from cache
                        if(cacheLast.isDefined) cacheLast else value
                      else
                        value
                    },
                    lastExCheckoutFieldName -> {
                      if (isCheckout) cacheLastExCheckout
                      else { // if not checkout and value is null, get from cache
                        if(value.isEmpty) // if value is null, get from cache
                          if(cacheLastExCheckout.isDefined) cacheLastExCheckout else value
                        else
                          value
                      }
                    }
                  )
                }
                else{
                  List(
                    firstEvarFieldName -> resolveToPrevious(dim, r.head._2(firstEvarFieldName), c.dims(dim)),
                    lastEvarFieldName -> resolveToCurrent(dim, r.head._2(lastEvarFieldName), c.dims(dim)),
                    if (isCheckout) {
                      lastExCheckoutFieldName -> r.head._2(lastExCheckoutFieldName)
                    }
                    else  {
                      lastExCheckoutFieldName -> resolveToCurrent(dim, r.head._2(lastExCheckoutFieldName), c.dims(dim))
                    }
                  )
                }

              // Uncomment to see evarization value movement
              //println(s"uuid: [${c.uuid}] dim: [${c.dims.getOrElse(dim, None)}] 1 >> ${output(0)}, 2 >> ${output(1)}, 3 >> ${output(2)}")
              output

          }.toMap[String, Option[Any]]
          (c.uuid, map) +: r
      }
      .toMap[String, Map[String, Option[Any]]]
  }



  def getEvarized[T: ClassTag](field: String, evarized: Map[String, Option[Any]]): Option[T] = {
   evarized.get(field) match {
     case Some(Some(v:T)) => Some(v)
     case _ => None
   }
  }
}