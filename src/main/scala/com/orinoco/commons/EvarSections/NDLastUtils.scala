package com.orinoco.commons.EvarSections

import com.orinoco.adapter.sessionize.cache.evar.NDLast
import com.orinoco.commons.EvarUtils.getEvarized

object NDLastUtils {
  /**
   * Objective:
   *    This script has one main purpose:
   *      1. Grab data to be evarized and apply them appropriately to the NDLast case class.
   **/

  def ndLastGrabber(evarized: Map[String, Option[Any]]): NDLast = NDLast(
    prior_domain_level_1_l = getEvarized[String]("prior_domain_level_1_l", evarized),
    prior_domain_level_2_l = getEvarized[String]("prior_domain_level_2_l", evarized),
    prior_domain_level_3_l = getEvarized[String]("prior_domain_level_3_l", evarized),
    prior_domain_level_4_l = getEvarized[String]("prior_domain_level_4_l", evarized),
    prior_domain_level_5_l = getEvarized[String]("prior_domain_level_5_l", evarized),
    prior_path_level_1_l = getEvarized[String]("prior_path_level_1_l", evarized),
    prior_path_level_2_l = getEvarized[String]("prior_path_level_2_l", evarized),
    prior_path_level_3_l = getEvarized[String]("prior_path_level_3_l", evarized),
    prior_path_level_4_l = getEvarized[String]("prior_path_level_4_l", evarized),
    prior_path_level_5_l = getEvarized[String]("prior_path_level_5_l", evarized),
    prior_pgn_level_1_l = getEvarized[String]("prior_pgn_level_1_l", evarized),
    prior_pgn_level_2_l = getEvarized[String]("prior_pgn_level_2_l", evarized),
    prior_pgn_level_3_l = getEvarized[String]("prior_pgn_level_3_l", evarized),
    prior_pgn_level_4_l = getEvarized[String]("prior_pgn_level_4_l", evarized),
    prior_pgn_level_5_l = getEvarized[String]("prior_pgn_level_5_l", evarized),
    prior_ssc_level_1_l = getEvarized[String]("prior_ssc_level_1_l", evarized),
    prior_ssc_level_2_l = getEvarized[String]("prior_ssc_level_2_l", evarized),
    prior_ssc_level_3_l = getEvarized[String]("prior_ssc_level_3_l", evarized),
    prior_ssc_level_4_l = getEvarized[String]("prior_ssc_level_4_l", evarized),
    prior_ssc_level_5_l = getEvarized[String]("prior_ssc_level_5_l", evarized),
    next_domain_level_1_l = getEvarized[String]("next_domain_level_1_l", evarized),
    next_domain_level_2_l = getEvarized[String]("next_domain_level_2_l", evarized),
    next_domain_level_3_l = getEvarized[String]("next_domain_level_3_l", evarized),
    next_domain_level_4_l = getEvarized[String]("next_domain_level_4_l", evarized),
    next_domain_level_5_l = getEvarized[String]("next_domain_level_5_l", evarized),
    next_path_level_1_l = getEvarized[String]("next_path_level_1_l", evarized),
    next_path_level_2_l = getEvarized[String]("next_path_level_2_l", evarized),
    next_path_level_3_l = getEvarized[String]("next_path_level_3_l", evarized),
    next_path_level_4_l = getEvarized[String]("next_path_level_4_l", evarized),
    next_path_level_5_l = getEvarized[String]("next_path_level_5_l", evarized),
    next_pgn_level_1_l = getEvarized[String]("next_pgn_level_1_l", evarized),
    next_pgn_level_2_l = getEvarized[String]("next_pgn_level_2_l", evarized),
    next_pgn_level_3_l = getEvarized[String]("next_pgn_level_3_l", evarized),
    next_pgn_level_4_l = getEvarized[String]("next_pgn_level_4_l", evarized),
    next_pgn_level_5_l = getEvarized[String]("next_pgn_level_5_l", evarized),
    next_ssc_level_1_l = getEvarized[String]("next_ssc_level_1_l", evarized),
    next_ssc_level_2_l = getEvarized[String]("next_ssc_level_2_l", evarized),
    next_ssc_level_3_l = getEvarized[String]("next_ssc_level_3_l", evarized),
    next_ssc_level_4_l = getEvarized[String]("next_ssc_level_4_l", evarized),
    next_ssc_level_5_l = getEvarized[String]("next_ssc_level_5_l", evarized)
  )
}