package com.orinoco.config.task

import com.orinoco.commons.MetaDataSnapshotsUtils.fromTsvLinestoIterString
import com.rakuten.rat.config.ElasticStatusUpdaterConfig
import com.typesafe.config.Config
import org.json4s.DefaultFormats
import org.json4s.native.Serialization._

import scala.io.Source
import scala.collection.JavaConverters._

case class ShopUrl(shop_id: BigInt, shop_url: String)

case class ItemGenreRecord(genre_id: String, genre_name: String, ancestor_genre: List[String])

case class NormalizeConfig(
                            env: String,
                            dataCenter: String,
                            elasticStatusUpdaterConfig: ElasticStatusUpdaterConfig,
                            hostName: String,
                            accsExemptFromIframeExclusionSet: Set[Int],
                            accsExemptFromScrollExclusionSet: Set[Int],
                            accAidsExemptFromIframeExclusionSeq: Seq[String],
                            accAidsExemptFromComplementedCVSeq: Seq[String],
                            filterServiceGroupsList: List[String],
                            inputPatternGeoIpDatabase: String,
                            inputPatternJapanGeoLookup: String,
                            inputPatternJson: String,
                            inputPatternRatShopIchiba: String,
                            inputPatternRatItemGenre: String,
                            inputPatternSocialNetwork: String,
                            inputPatternCacheCustomerDNA: String,
                            inputSnapshotCompanyPath: String,
                            inputSnapshotHiveNamePath: String,
                            inputSnapshotServiceCategoryPath: String,
                            inputSnapshotServiceGroupRemapping: Map[String, String],
                            inputSnapshotServiceGroupPath: String,
                            inputSnapshotProcessingBucketPath: String,
                            pgtList: List[String],
                            outputPatternNormalize: String,
                            outputPatternExceptions: String
                          ) {
  def dump(): String = write(this)(DefaultFormats)
}
object NormalizeConfig {
  def load(mainConfig: Config): NormalizeConfig = {
    val config = mainConfig.getConfig("normalize")

    NormalizeConfig(
      env = mainConfig.getString("env"),
      dataCenter = mainConfig.getString("data-center"),
      hostName = mainConfig.getString("host-name"),
      elasticStatusUpdaterConfig = ElasticStatusUpdaterConfig.load(mainConfig.getConfig("elastic")),
      accsExemptFromScrollExclusionSet = config.getIntList("accs-exempt-from-scroll-exclusion").asScala.map(_.toInt).toSet,
      accsExemptFromIframeExclusionSet = config.getIntList("accs-exempt-from-iframe-exclusion").asScala.map(_.toInt).toSet,
      accAidsExemptFromIframeExclusionSeq = config.getStringList("acc_aids-exempt-from-iframe-exclusion").asScala,
      accAidsExemptFromComplementedCVSeq = config.getStringList("acc_aids-exempt-from-complemented-cv").asScala.toList,
      filterServiceGroupsList = config.getStringList("filter-service-groups").asScala.toList,
      pgtList = config.getStringList("pgt-list").asScala.toList,
      inputPatternJson = config.getString("input-pattern.json"),
      inputPatternSocialNetwork = config.getString("input-pattern.social-network-path"),
      inputSnapshotCompanyPath = config.getString("input-pattern.snapshot.company"),
      inputSnapshotHiveNamePath = config.getString("input-pattern.snapshot.hive-name"),
      inputSnapshotServiceCategoryPath = config.getString("input-pattern.snapshot.service-category"),
      inputSnapshotServiceGroupPath = config.getString("input-pattern.snapshot.service-group"),
      inputSnapshotServiceGroupRemapping = fromTsvLinesToIterString(resourceFileToList(config.getString("input-pattern.service-group-remapping"))).toMap,
      inputSnapshotProcessingBucketPath = config.getString("input-pattern.snapshot.processing-bucket"),
      inputPatternGeoIpDatabase = config.getString("input-pattern.geo-ip-database"),
      inputPatternJapanGeoLookup = config.getString("input-pattern.japan-geo-lookup"),
      inputPatternRatShopIchiba = config.getString("input-pattern.rat-shop-ichiba"),
      inputPatternRatItemGenre = config.getString("input-pattern.rat-item-genre"),
      inputPatternCacheCustomerDNA = config.getString("input-pattern.cache-customer-dna"),
      outputPatternNormalize = config.getString("output-pattern.normalize"),
      outputPatternExceptions = config.getString("output-pattern.exceptions")
    )
  }

  def resourceFileToList(filePath: String): List[String] = {
    val source = Source.fromSource(filePath)
    val result = source
      .getLines()
      .toList
    source.close()
    result
  }

}