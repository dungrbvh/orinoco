package com.orinoco.commons

import com.orinoco.adapter.sessionize.SessionizePayload

object  SessionizeRowUtils {
  /**
   * Objective:
   *    This script has one main purposes:
   *      1. Provide uniform calculations that may be repeatedly called upon in various jobs.
   *
   * Notes:
   *    These calculations are part of a wider set of data transformations.
   *    Each calculation is made off the current row of the users' action.
   *    Please refer to those respective transformations / calculations for further details.
   **/

  final val checkoutPgtList = List("cart_checkout", "cart_modify", "shop_item")

  // Returns true if event type for current user action is 'pv'
  def checkoutFinder(currentRow: SessionizePayload): Boolean =
    checkoutPgtList.contains(currentRow.norm.pgt.getOrElse(""))

  // Returns true if event type for current user action is 'pv'
  def pageViewFinder(currentRow: SessionizePayload): Boolean = currentRow.norm.event_type.equals("pv")

  // Returns true if service category is 'parts'
  def serviceCategoryPartsFinder(currentRow: SessionizePayload): Boolean =
    currentRow.norm.service_category.getOrElse("").equals("parts")

  // Returns true if service category is 'parts'
  def webFinder(currentRow: SessionizePayload): Boolean =
    currentRow.norm.app_type.getOrElse("").equals("web")
}