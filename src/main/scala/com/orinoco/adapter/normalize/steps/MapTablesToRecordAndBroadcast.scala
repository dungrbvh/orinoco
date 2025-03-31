package com.orinoco.adapter.normalize.steps

import com.orinoco.commons.PathUtils.{getFs, pathDate, returnRecentDT}
import com.orinoco.config.task.{ItemGenreRecord, NormalizeConfig, ShopUrl}
import com.orinoco.schema.normalize.{ Parsed }
import com.orinoco.schema.cdna.CustomerDNA
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}

import scala.io.Source
import scala.util.Try

case class MapTablesToRecordAndBroadcast(
                                          socialNetworkPath: String,
                                          companyMapping: Map[Int, String],
                                          serviceCategoryMapping: Map[Int, String],
                                          serviceGroupTimezoneExceptions: Seq[String],
                                          serviceGroupMapping: Map[Int, String],
                                          serviceGroupRemapping: Map[(Int, Int), String],
                                          itemGenreMapping: Map[String, String],
                                          shopUrlMapping: Map[Int, String],
                                          cdnaMapping: Dataset[CustomerDNA],
                                          geoIpLookupByteArray: Array[Byte],
                                          japanGeoLookup: Map[String, JapanGeoLookup],
                                          pgtList: List[String],
                                          complementedCvAccAidExemptions: Seq[String]
                                        )

case class JapanGeoLookup(
                           postal_code: Option[String] = None,
                           region_1: Option[String] = None,
                           region_2: Option[String] = None,
                           region_3: Option[String] = None,
                           region_4: Option[String] = None,
                           latitude: Option[String] = None,
                           longitude: Option[String] = None,
                           elevation: Option[String] = None
                         )

object MapTablesToRecordAndBroadcast {
  def apply(
           filteredDataset: Dataset[Parsed],
           normalizeConfig: NormalizeConfig,
           inputSnapshotCompanyMapping: Map[String, String],
           inputSnapshotServiceGroupTimezoneList: Map[String, String],
           inputSnapshotServiceCategoryMapping: Map[Int, String],
           inputSnapshotServiceGroupMapping: Map[Int, String],
           env: String,
           hourDT: DateTime
           )(implicit spark: SparkSession): Broadcast[MapTablesToRecordAndBroadcast] = {
    // Some information is retrieved from configs, the rest must be retrieved via spark.sql.\
    val serviceGroupJSTExceptions: Seq[String] = inputSnapshotServiceGroupTimezoneList
      .filter(_._2.equals("utc")).keys.toSeq

    /** Hive cuts data at the end for the given day. Need to cache day's lookup based on prior date.
     * Furthermore, Hive is not guaranteed to cut Hive data immediately at close of day. For safety purposes,
     * one more day is subtracted as a buffer. Cache date corresponds to the event date and has nothing
     * to do with the date of the underlying Hive lookup data.
     **/
    val hiveLookupDataDayDt = hourDT.minusDays(2)

    val inputPathRatItemShopIchiba =
      if (env.contains("stg"))
        returnRecentDT(normalizeConfig.inputPatternRatShopIchiba, getFs(normalizeConfig.inputPatternRatShopIchiba))
      else
        pathDate(normalizeConfig.inputPatternRatShopIchiba, hiveLookupDataDayDt)

    val shopUrlMapping = getShopUrlMapping(
      filteredDataset,
      inputSnapshotServiceGroupMapping,
      inputPathRatItemShopIchiba
    )

    val itemGenreMapping = getItemGenreMapping(pathDate(normalizeConfig.inputPatternRatItemGenre, hiveLookupDataDayDt))
    val customerDNA = getCDNA(
      filteredDataset,
      normalizeConfig.env,
      pathDate(normalizeConfig.inputPatternCacheCustomerDNA, hourDT.withZone(DateTimeZone.UTC).minusDays(2))
    )

    // These need to be retrieved from spark's filesystem
    val geoIpLookupByteArray = getGeoIpDatabaseByteArray(normalizeConfig.inputPatternGeoIpDatabase,
      getFs(normalizeConfig.inputPatternGeoIpDatabase))
    val japanGeoLookups = getJapanGeoLookups(normalizeConfig.inputPatternJapanGeoLookup,
      getFs(normalizeConfig.inputPatternJapanGeoLookup))
    val companyMappingKeyChanged = inputSnapshotCompanyMapping
      .flatMap {
        case(key, value) => Try {
          val keyArray = key.split("_")
          if(keyArray.size > 1)
            keyArray(0).toInt -> value
          else
            key.toInt -> value
        }.toOption
      }
    val serviceGroupRemappingKeyChanged = normalizeConfig
      .inputSnapshotServiceGroupRemapping
      .flatMap {
        case(key, value) => Try {
          val keyArray = key.split("_")
          (keyArray(0).toInt, keyArray(1).toInt) -> value
        }.toOption
      }

    // Build object with broadcast information
    spark.SparkContext.broadcast(
      MapTablesToRecordAndBroadcast(
        socialNetworkPath = normalizeConfig.inputPatternSocialNetwork,
        companyMapping = companyMappingKeyChanged,
        serviceCategoryMapping = inputSnapshotServiceCategoryMapping,
        serviceGroupTimezoneExceptions = serviceGroupJSTExceptions,
        serviceGroupMapping = inputSnapshotServiceGroupMapping,
        serviceGroupRemapping = serviceGroupRemappingKeyChanged,
        itemGenreMapping = itemGenreMapping,
        shopUrlMapping = shopUrlMapping,
        cdnaMapping = customerDNA,
        geoIpLookupByteArray = geoIpLookupByteArray,
        japanGeoLookup = japanGeoLookups,
        pgtList = normalizeConfig.pgtList,
        complementedCvAccAidExemptions = normalizeConfig.accAidsExemptFromComplementedCVSeq
      )
    )
  }
  /**
   * Reads genre item information from HDFS path into List[ItemGenreRecord].
   * Transforms records of "ItemGenreRecord(genre_id, genre_name, ancestor_genre)",
   * where ancestor_genre is a List[genre_id],
   * into a Map(genre_id -> ancestor_genres_with_names)
   * where ancestor_genres_with_names is a String of ids of ancestor_genres concatenated with names
   *
   * @example input List elem: ItemGenreRecord(566611,挽肉,List(100227, 100228))
   * @example output Map elem: (566611 -> 食品(100227)>精肉・肉加工品(100228)>挽肉(566611))
   */
  private def getItemGenreMapping(cached_rat_item_genre: String)(implicit spark: SparkSession): Map[String, String] = {
    import spark.implicits._

    val itemGenre = spark
      .read.parquet(cached_rat_item_genre)
      .select($"genre_id",
        $"genre_name",
        $"ancestor_genre")
      .map(row => ItemGenreRecord(row.getString(0), row.getString(1), row.getSeq[String](2).toList))

    val itemGenreIdToName = itemGenre.map(x => (x.genre_id, x.genre_name)).toMap[String, String]

    itemGenre.map {
      itemGenre => itemGenre.genre_id -> (itemGenre.ancestor_genre ::: List(itemGenre.genre_id))
        .filter(_.nonEmpty)
        .map(a => itemGenreIdToName(a) + s"($a)")
        .mkString(">")
    }.toMap
  }

  /* Reads shop information from HDFS paths */
  private def getShopUrlMapping(
                               filteredDataset: Dataset[Parsed],
                               serviceGroupMapping: Map[Int, String],
                               inputPathRatItemShopIchiba: String
                               )(implicit spark: SparkSession): Map[Int, String] = {
    import spark.implicits._

    val targetShopIds = filteredDataset
      .filter(_.acc.flatMap(key => serviceGroupMapping.get(key)).contains("ichiba"))
      .map(parsed => getShopIdPurchase(parse.cv.getOrElse(Map.empty), parsed.shopid_itemid))
      .filter(_.nonEmpty)
      .distinct()

    spark
      .read
      .format("orc")
      .load(inputPathRatItemShopIchiba)
      .createOrReplaceTempView("targetShopIds")

    spark.sql(
      """
        |SELECT DISTINCT
        |    shop_id,
        |    shop_url
        |FROM
        |   shop_ichiba_table
        |WHERE
        |   shop_id IN (SELECT * FROM targetShopIds)
      """.stripMargin
    ).as[ShopUrl]
      .collect()
      .map(x => (x.shop_id.toInt, x.shop_url))
      .toMap[Int, String]
  }

  /* Reads customer DNA information from HDFS paths */
  private def getCDNA(
                     filteredDataset: Dataset[Parsed], env: String, cacheCDNAPath: String)(implicit  spark: sparkSession): Dataset[CustomerDNA] = {
    import spark.implicits._
    if (env.contains("stg")) spark.emptyDataset[CustomerDNA]
    else {
      spark
        .read
        .parquet(cacheCDNAPath)
        .createOrReplaceTempView("cached_cdna_table_partitioned_by_easy_id")

      val targetEasyIds: Dataset[String] = filteredDataset
        .filter(_.easyid.nonEmpty)
        .map(_.easyid.get)
        .distinct()

    }
    // Creates list of easy IDs found in raw JSON
    targetEasyIds.createOrReplaceTempView("targetEasyIds")

    spark.sql(
        """
          |SELECT
          |    *
          |FROM
          |    cached_cdna_table_partitioned_by_easy_id
          |WHERE cdna_easy_id IN (SELECT DISTINCT * FROM targetEasyIds)
        """.stripMargin
      )
      .as[CustomerDNA]
  }

  private def getShopIdPurchase(cv: Map[String, Int], shopid_itemid: Option[Array[Option[String]]]): Option[Int] = {
    if (cv.contains("purchase_gms") && cv("purchase_gms") > 0)
      Try(shopid_itemid.get.head.getOrElse("").split("/").head.toInt).toOption
    else None
  }

  private def getGeoIpDatabaseByteArray(geoIpDatabaseBytePath: String, fs: FileSystem): Array[Byte] = {
    if (!fs.exists(new Path(geoIpDatabaseBytePath)))
      throw new Exception(s"Path '$geoIpDatabaseBytePath' does not exist!")

    IOUtils.toByteArray(fs.open(new Path(geoIpDatabaseBytePath)))
  }

  private def getJapanGeoLookups(japanGeoLookupPath: String, fs: FileSystem): Map[String, JapanGeoLookup] = {
    val source =
      Source.fromInputStream(
        IOUtils.toBufferedInputStream(fs.open(new Path(japanGeoLookupPath))))
    val getJapanGeoLookupRows = source.getLines.toList
    source.close()

    val indices = getJapanGeoLookupRows.head.split(";").zipWithIndex.toMap
    (for {
      row <- getJapanGeoLookupRows.tail // Skip header row
      cols = row.split(";")
      if cols(indices("language")) == "EN"
      lookup = JapanGeoLookup(
        postal_code = Option(cols(indices("postcode"))),
        region_1 = Option(cols(indices("region1"))),
        region_2 = Option(cols(indices("region2"))),
        region_3 = Option(cols(indices("region3"))),
        region_4 = Option(cols(indices("region4"))),
        latitude = Option(cols(indices("latitude"))),
        longitude = Option(cols(indices("longitude"))),
        elevation = Try(Option(cols(indices("elevation")))).getOrElse(None))
    } yield (cols(indices("postcode")), lookup))
      .sortBy(x => (x._2.region_1, x._2.region_2, x._2.region_3, x._2.region_4))
      .groupBy(_._1)
      .map(_._2.head)
  }
}