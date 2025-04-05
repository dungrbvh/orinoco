package com.orinoco.commons.EvarSections

import com.orinoco.adapter.sessionize.cache.evar.NDLastExCheckout
import com.orinoco.commons.EvarUtils.getEvarized

object NDLastExCheckoutUtils {
  /**
   * Objective:
   *    This script has one main purpose:
   *      1. Grab data to be evarized and apply them appropriately to the NDLastExCheckout case class.
   **/

  def ndLastExCheckoutGrabber(evarized: Map[String, Option[Any]]): NDLastExCheckout = NDLastExCheckout(
    prior_domain_level_1_l_ex_checkout = getEvarized[String]("prior_domain_level_1_l_ex_checkout", evarized),
    prior_domain_level_2_l_ex_checkout = getEvarized[String]("prior_domain_level_2_l_ex_checkout", evarized),
    prior_domain_level_3_l_ex_checkout = getEvarized[String]("prior_domain_level_3_l_ex_checkout", evarized),
    prior_domain_level_4_l_ex_checkout = getEvarized[String]("prior_domain_level_4_l_ex_checkout", evarized),
    prior_domain_level_5_l_ex_checkout = getEvarized[String]("prior_domain_level_5_l_ex_checkout", evarized),
    prior_path_level_1_l_ex_checkout = getEvarized[String]("prior_path_level_1_l_ex_checkout", evarized),
    prior_path_level_2_l_ex_checkout = getEvarized[String]("prior_path_level_2_l_ex_checkout", evarized),
    prior_path_level_3_l_ex_checkout = getEvarized[String]("prior_path_level_3_l_ex_checkout", evarized),
    prior_path_level_4_l_ex_checkout = getEvarized[String]("prior_path_level_4_l_ex_checkout", evarized),
    prior_path_level_5_l_ex_checkout = getEvarized[String]("prior_path_level_5_l_ex_checkout", evarized),
    prior_pgn_level_1_l_ex_checkout = getEvarized[String]("prior_pgn_level_1_l_ex_checkout", evarized),
    prior_pgn_level_2_l_ex_checkout = getEvarized[String]("prior_pgn_level_2_l_ex_checkout", evarized),
    prior_pgn_level_3_l_ex_checkout = getEvarized[String]("prior_pgn_level_3_l_ex_checkout", evarized),
    prior_pgn_level_4_l_ex_checkout = getEvarized[String]("prior_pgn_level_4_l_ex_checkout", evarized),
    prior_pgn_level_5_l_ex_checkout = getEvarized[String]("prior_pgn_level_5_l_ex_checkout", evarized),
    prior_ssc_level_1_l_ex_checkout = getEvarized[String]("prior_ssc_level_1_l_ex_checkout", evarized),
    prior_ssc_level_2_l_ex_checkout = getEvarized[String]("prior_ssc_level_2_l_ex_checkout", evarized),
    prior_ssc_level_3_l_ex_checkout = getEvarized[String]("prior_ssc_level_3_l_ex_checkout", evarized),
    prior_ssc_level_4_l_ex_checkout = getEvarized[String]("prior_ssc_level_4_l_ex_checkout", evarized),
    prior_ssc_level_5_l_ex_checkout = getEvarized[String]("prior_ssc_level_5_l_ex_checkout", evarized),
    next_domain_level_1_l_ex_checkout = getEvarized[String]("next_domain_level_1_l_ex_checkout", evarized),
    next_domain_level_2_l_ex_checkout = getEvarized[String]("next_domain_level_2_l_ex_checkout", evarized),
    next_domain_level_3_l_ex_checkout = getEvarized[String]("next_domain_level_3_l_ex_checkout", evarized),
    next_domain_level_4_l_ex_checkout = getEvarized[String]("next_domain_level_4_l_ex_checkout", evarized),
    next_domain_level_5_l_ex_checkout = getEvarized[String]("next_domain_level_5_l_ex_checkout", evarized),
    next_path_level_1_l_ex_checkout = getEvarized[String]("next_path_level_1_l_ex_checkout", evarized),
    next_path_level_2_l_ex_checkout = getEvarized[String]("next_path_level_2_l_ex_checkout", evarized),
    next_path_level_3_l_ex_checkout = getEvarized[String]("next_path_level_3_l_ex_checkout", evarized),
    next_path_level_4_l_ex_checkout = getEvarized[String]("next_path_level_4_l_ex_checkout", evarized),
    next_path_level_5_l_ex_checkout = getEvarized[String]("next_path_level_5_l_ex_checkout", evarized),
    next_pgn_level_1_l_ex_checkout = getEvarized[String]("next_pgn_level_1_l_ex_checkout", evarized),
    next_pgn_level_2_l_ex_checkout = getEvarized[String]("next_pgn_level_2_l_ex_checkout", evarized),
    next_pgn_level_3_l_ex_checkout = getEvarized[String]("next_pgn_level_3_l_ex_checkout", evarized),
    next_pgn_level_4_l_ex_checkout = getEvarized[String]("next_pgn_level_4_l_ex_checkout", evarized),
    next_pgn_level_5_l_ex_checkout = getEvarized[String]("next_pgn_level_5_l_ex_checkout", evarized),
    next_ssc_level_1_l_ex_checkout = getEvarized[String]("next_ssc_level_1_l_ex_checkout", evarized),
    next_ssc_level_2_l_ex_checkout = getEvarized[String]("next_ssc_level_2_l_ex_checkout", evarized),
    next_ssc_level_3_l_ex_checkout = getEvarized[String]("next_ssc_level_3_l_ex_checkout", evarized),
    next_ssc_level_4_l_ex_checkout = getEvarized[String]("next_ssc_level_4_l_ex_checkout", evarized),
    next_ssc_level_5_l_ex_checkout = getEvarized[String]("next_ssc_level_5_l_ex_checkout", evarized)
  )
}