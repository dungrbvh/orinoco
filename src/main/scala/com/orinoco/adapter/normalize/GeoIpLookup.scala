package com.orinoco.adapter.normalize

import java.io.InputStream
import java.net.InetAddress

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse

import scala.util.Try

object GeoIpLookup {
  def apply(databaseInputStream: Option[InputStream]): GeoIpLookup =
    databaseInputStream.fold(empty)(is => new DatabaseBackedGeoIpLookup(is))

  def empty: GeoIpLookup = EmptyGeoIpLookup
}

trait GeoIpLookup extends Serializable {
  def cityResponse(ip: String): Option[CityResponse]

  def city(cityResponse: Option[CityResponse]): Option[String]

  def country(cityResponse: Option[CityResponse]): Option[String]

  def postalCode(cityResponse: Option[CityResponse]): Option[String]
}

object EmptyGeoIpLookup extends GeoIpLookup {
  def cityResponse(ip: String): Option[CityResponse] = None

  def city(cityResponse: Option[CityResponse]): Option[String] = None

  def country(cityResponse: Option[CityResponse]): Option[String] = None

  def postalCode(cityResponse: Option[CityResponse]): Option[String] = None
}

private class DatabaseBackedGeoIpLookup(databaseInputStream: InputStream) extends GeoIpLookup {
  private val databaseReader = new DatabaseReader.Builder(databaseInputStream).withCache(new CHMCache()).build()

  def cityResponse(ip: String): Option[CityResponse] = Try(databaseReader.city(InetAddress.getByName(ip))).toOption

  def city(cityResponse: Option[CityResponse]): Option[String] = {
    if (cityResponse.nonEmpty) {
      try {
        Option(cityResponse.get.getCity.getName)
      } catch {
        case e: Exception => None
      }
    } else None
  }

  def country(cityResponse: Option[CityResponse]): Option[String] = {
    if (cityResponse.nonEmpty) {
      try {
        Option(cityResponse.get.getCountry.getName)
      } catch {
        case e: Exception => None
      }
    } else None
  }

  def postalCode(cityResponse: Option[CityResponse]): Option[String] = {
    if (cityResponse.nonEmpty) {
      try {
        Option(cityResponse.get.getPostal.getCode)
      } catch {
        case e: Exception => None
      }
    } else None
  }

}