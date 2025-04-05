package com.orinoco.application.sessionize.post

import com.orinoco.adapter.postcalculate.cache.PostCalculateCache
import com.orinoco.adapter.postcalculate.nextdimension._
import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.adapter.sessionize.sections.{PostCuttingSection, PreSessionDataSection, SessionDataSection}
import com.orinoco.commons.SessionizeRowUtils.checkoutPgtList
import com.orinoco.schema.normalize.extended.{GroupedCarrierIdentification, GroupedMediaVideo}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

package object evar {
  val CachePrefix = "sc."
  val PageTypeFieldName = "pgt"
  val FirstEvarFieldSuffix = "_f"
  val LastEvarFieldSuffix = "_l"
  val LastExCheckoutEvarFieldSuffix = "_l_ex_checkout"
  private val emptyMap = Map[String, Option[Any]]()

  // Add to list of non Map[String, String] Evar Variables. Helps to process through removeBlankOrNullValues function.
  final val nonMapSSList = List("loc")

  case class Evarizing(ts: Int, uuid: String, dims: Map[String, Option[Any]], cache: Map[String, Option[Any]])

  private def caseClassToMap(cc: Product): Map[String, Option[Any]] =
    cc.getClass.getDeclaredFields.map(_.getName).zip(cc.productIterator.map{
      case o:Option[_] => o
      case other => Option(other)
    }.to).toMap

  private def sessionizePayloadToGenericRowWithSchema(
                                                     rec: SessionizePayload,
                                                     evarizationSchema: StructType
                                                     ): GenericRowWithSchema =
    new GenericRowWithSchema(sessionizedToRow(rec, evarizationSchema), evarizationSchema)

  private def sessionizedToRow(rec: SessionizePayload, evarizationSchema: StructType): Array[Any] = {

    val normMap = caseClassToMap(rec.norm)
    val groupedMV = caseClassToMap(rec.norm.grouped_media_video.getOrElse(GroupedMediaVideo.empty))
    val groupedCarrId = caseClassToMap(rec.norm.grouped_carrier_id.getOrElse(GroupedCarrierIdentification.empty))
    val cdnaMap = caseClassToMap(rec.cdna)
    val preMap =  caseClassToMap(rec.pre_session_data_section.getOrElse(PreSessionDataSection.empty))
    val sessMap = caseClassToMap(rec.session_data_section.getOrElse(SessionDataSection.empty))
    val postMap = caseClassToMap(rec.post_cutting_section.getOrElse(PostCuttingSection.empty))
    val nextMap = caseClassToMap(rec.dimensional_data_section.getOrElse(DimensionalDataSection.empty)
      .next_dimensions.getOrElse(NextDimensions.empty))
    val prevMap = caseClassToMap(rec.dimensional_data_section.getOrElse(DimensionalDataSection.empty)
      .prev_dimensions.getOrElse(PriorDimensions.empty))

    // projection push down
    evarizationSchema.fieldNames.map(
      x => {
        if (normMap.contains(x)) normMap(x)
        else if (groupedMV.contains(x)) groupedMV(x)
        else if (groupedCarrId.contains(x)) groupedCarrId(x)
        else if (cdnaMap.contains(x)) cdnaMap(x)
        else if (preMap.contains(x)) preMap(x)
        else if (sessMap.contains(x)) sessMap(x)
        else if (postMap.contains(x)) postMap(x)
        else if (nextMap.contains(x)) nextMap(x)
        else if (prevMap.contains(x)) prevMap(x)
        else throw new Exception(s"Can't find source evar value: $x")
      }
    )
  }

  def setEvarSchema(list: List[String]): StructType = {
    StructType(
      List(
        StructField("time_stamp_epoch", IntegerType),
        StructField("uuid", StringType),
        StructField(evar.PageTypeFieldName, StringType)
      ) :::
        list.map(x => StructField(x, StringType, nullable = true))
    )
  }

  /*
  scala> getCCParams(C("bob", 1, true, 2.0, Array(100, 200), Array("cat", "dog"), Array(1.0, 2.0), Map[String, String]("q" -> "qq")))
  res8: scala.collection.immutable.Map[String,Any] = Map(e -> Array(100, 200), f -> Array(cat, dog), a -> bob, b -> 1, g -> Array(1.0, 2.0), c -> true, h -> Map(q -> qq), d -> 2.0)
 */
  def ccToMap(cc: Product): Map[String, Option[Any]] = {
    val values = cc.ProductIterator
    cc.getClass.getDeclaredFields.map( _.getName -> {
      values.next match {
        case option:Option[Any] => option
        case other => Option(other)
      }
    }).toMap
  }

  /**
   * Convert record for evarization cache
   *
   * @param evarCache Caches Evarization records
   * @return
   *
   * Note: If the EvarSection case class grows, those fields must also be added below
   **/
  def evarToMap(evarCache: Option[EvarSection]): Map[String, Option[Any]] = {
    // Maps general data
    ccToMap(evarCache.get.first) ++ ccToMap(evarCache.get.last) ++ ccToMap(evarCache.get.last_ex_checkout) ++
    // Maps cdna data
    ccToMap(evarCache.get.cdna_first) ++ ccToMap(evarCache.get.cdna_last) ++
    //Maps next dimension data
    ccToMap(evarCache.get.nd_first) ++ ccToMap(evarCache.get.nd_last) ++ ccToMap(evarCache.get.nd_last_ex_checkout)
  }

  /**
   * sessionizedRecordsToEvarizable and postCalculatedRecordsToEvarizable operate in three steps:
   *    1. Filter on 'pv' events.
   *    2. Check if session cache exists and apply evar mapping function if session ID of cache matches base session ID.
   *    3. Apply evar mapping if base value exists.
   *
   * @param recs                Session of user
   * @param evarizationSchema   Redefined evarization schema
   * @return
   **/
  def sessionizedRecordsToEvarizable(recs: Seq[SessionizePayload], evarizationSchema: StructType): Seq[Evarizing] = {
    val genericEvarSchema = recs // Returns Seq[(GenericRowWithSchema, Map[String, Option[Any]])]
      .filter(_.norm.event_type == "pv")
      .map { x =>
        val sessionizationCache = x.sc.getOrElse(SessionizationCache.empty)
        val baseSessionId = x.session_data_section.getOrElse(SessionDataSection.empty).session_id

        val evarMap = // Create mapping of evarization values from cached data
          if (x.sc.isEmpty || sessionizationCache.evar_cache.isEmpty) emptyMap // Return empty evar schema if no cache
          else // Match both base session ID and cache session ID
            if (baseSessionId.contains(sessionizationCache.session_id.getOrElse("")))
              evarCCToMap(sessionizationCache.evar_cache) // Cached sessionized data retrieved
            else emptyMap // Return empty evar schema to indicate new session ID

        (sessionizePayloadToGenericRowWithSchema(x, evarizationSchema), evarMap)
      }

    evarizingSequence(genericEvarSchema, evarizationSchema) // Redefine evar schema of only specified fields
  }

  def postCalculatedRecordsToEvarizable(recs: Seq[SessionizePayload], evarizationSchema: StructType): Seq[Evarizing] = {
    val genericEvarSchema = recs // Returns Seq[(GenericRowWithSchema, Map[String, Option[Any]])]
      .filter(_.norm.event_type == "pv")
      .map { x =>
        val postCalcCache = x.post_calculate_cache.getOrElse(PostCalculateCache.empty)
        val baseSessionId = x.session_data_section.getOrElse(SessionDataSection.empty).session_id

        val evarMap = // Create mapping of evarization values from cached data
          if (x.post_calculate_cache.isEmpty || postCalcCache.evar_cache.isEmpty)
            emptyMap // Return empty evar schema if no cache
          else // Match both base session ID and cache session ID
            if (baseSessionId.contains(postCalcCache.session_id))
              evarCCToMap(postCalcCache.evar_cache) // Cached post calculated data retrieved if session IDs match
            else emptyMap // Return empty evar schema to indicate new session ID

        (sessionizePayloadToGenericRowWithSchema(x, evarizationSchema), evarMap)
      }

    evarizingSequence(genericEvarSchema, evarizationSchema) // Redefine evar schema of only specified fields
  }

  /**
   * Returns redefined evar schema specified in evarizationSchema
   *
   * @param genericEvarSchema   Base evarization schema
   * @param evarizationSchema   Redefined evarization schema
   * @return
   **/
  def evarizingSequence(
                         genericEvarSchema: Seq[(GenericRowWithSchema, Map[String, Option[Any]])], evarizationSchema: StructType
                       ): Seq[Evarizing] =
    genericEvarSchema.map {
      case (_payload: GenericRowWithSchema, cache: Map[String, Option[Any]]) =>

        val payload = _payload.getValuesMap[Option[Any]](evarizationSchema.fieldNames)
        (payload.get("time_stamp_epoch"), payload.get("uuid")) match {
          case (Some(Some(tse: Int)), Some(Some(uuid: String))) =>
            Evarizing(tse, uuid, payload, cache)
          case _ =>
            throw new Exception("Could not get 'time_stamp_epoch' and 'uuid' from payload to build Evarization.")
        }
    }

  def mergeMap[T: ClassTag](toThis: Map[String, T], withThis: Map[String, T]): Map[String, T] = {
    toThis ++ withThis.filterKeys(k => !toThis.contains(k))
  }

  def removeBlankOrNullValues(m: Map[String, String]): Map[String, String] = {
    m.filterNot(kv => kv._2 == null || kv._2.isEmpty || kv._2 == "null")
  }

  def resolveToCurrent(dim: String, previous: Option[Any], current: Option[Any]): Option[Any] = {
    (nonMapSSList.contains(dim), previous, current) match {
      /** there is previous and new values if Map type */
      case (false, Some(prevVal: Map[String, String] @unchecked), Some(newVal: Map[String, String] @unchecked)) =>
        val withValNewVal = removeBlankOrNullValues(newVal)
        val withValPrevVal = removeBlankOrNullValues(prevVal)
        Some(mergeMap(withValNewVal, withValPrevVal))

      case (true, Some(prevVal: Map[String, Double] @unchecked), Some(newVal: Map[String, Double] @unchecked)) =>
        Some(mergeMap(newVal, prevVal))

      /** only having the previous value */
      case (_, Some(_), None) =>
        cleanIfMap(dim, previous)

      /** for all other types, as long as there is a current value */
      case (_, _, Some(_)) =>
        cleanIfMap(dim, current)

      /** for very obvious reasons */
      case (_, None, None) =>
        None
    }
  } // end of resolveToCurrent

  def resolveToPrevious(dim: String, previous: Option[Any], current: Option[Any]): Option[Any] = {
    (nonMapSSList.contains(dim), previous, current) match {
      /** there is previous and new values if Map type */
      case (false, Some(prevVal: Map[String, String] @unchecked), Some(newVal: Map[String, String] @unchecked)) =>
        val withValNewVal = removeBlankOrNullValues(newVal)
        val withValPrevVal = removeBlankOrNullValues(prevVal)
        Some(mergeMap(withValPrevVal, withValNewVal))

      case (true, Some(prevVal: Map[String, Double] @unchecked), Some(newVal: Map[String, Double] @unchecked)) =>
        Some(mergeMap(newVal, prevVal))

      /** for all other types, as long as there is a previous value */
      case (_, Some(_), _) =>
        cleanIfMap(dim, previous)

      /** only having the current value */
      case (_, None, Some(_)) =>
        cleanIfMap(dim, current)

      /** for very obvious reasons */
      case (_, None, None) =>
        None
    }
  } // end of resolveToPrevious

  def getTypeOrNone[T:ClassTag](any:Any):Option[T] =  any match {
    case t:T => Some(t)
    case _ => None
  }

  def cleanIfMap(dim: String, obj: Option[Any]): Option[Any] = (nonMapSSList.contains(dim), obj) match {
    case (false, Some(v: Map[String, String] @unchecked)) => Some(removeBlankOrNullValues(v))
    case _ => obj
  }

  // Check if the page type is a check out
  def checkoutFinder(pageType: Option[Any]): Boolean =
    checkoutPgtList.contains(pageType.getOrElse("").toString)
}
