package com.orinoco.commons.EvarSections

import com.orinoco.adapter.sessionize.cache.evar.NDFirst
import com.orinoco.commons.EvarUtils.getEvarized

object NDFirstUtils {
  /**
   * Objective:
   *    This script has one main purpose:
   *      1. Grab data to be evarized and apply them appropriately to the NDFirst case class.
   **/

  def ndFirstGrabber(evarized: Map[String, Option[Any]]): NDFirst = NDFirst(
    prior_domain_level_1_f = getEvarized[String]("prior_domain_level_1_f", evarized),
    prior_domain_level_2_f = getEvarized[String]("prior_domain_level_2_f", evarized),
    prior_domain_level_3_f = getEvarized[String]("prior_domain_level_3_f", evarized),
    prior_domain_level_4_f = getEvarized[String]("prior_domain_level_4_f", evarized),
    prior_domain_level_5_f = getEvarized[String]("prior_domain_level_5_f", evarized),
    prior_path_level_1_f = getEvarized[String]("prior_path_level_1_f", evarized),
    prior_path_level_2_f = getEvarized[String]("prior_path_level_2_f", evarized),
    prior_path_level_3_f = getEvarized[String]("prior_path_level_3_f", evarized),
    prior_path_level_4_f = getEvarized[String]("prior_path_level_4_f", evarized),
    prior_path_level_5_f = getEvarized[String]("prior_path_level_5_f", evarized),
    prior_pgn_level_1_f = getEvarized[String]("prior_pgn_level_1_f", evarized),
    prior_pgn_level_2_f = getEvarized[String]("prior_pgn_level_2_f", evarized),
    prior_pgn_level_3_f = getEvarized[String]("prior_pgn_level_3_f", evarized),
    prior_pgn_level_4_f = getEvarized[String]("prior_pgn_level_4_f", evarized),
    prior_pgn_level_5_f = getEvarized[String]("prior_pgn_level_5_f", evarized),
    prior_ssc_level_1_f = getEvarized[String]("prior_ssc_level_1_f", evarized),
    prior_ssc_level_2_f = getEvarized[String]("prior_ssc_level_2_f", evarized),
    prior_ssc_level_3_f = getEvarized[String]("prior_ssc_level_3_f", evarized),
    prior_ssc_level_4_f = getEvarized[String]("prior_ssc_level_4_f", evarized),
    prior_ssc_level_5_f = getEvarized[String]("prior_ssc_level_5_f", evarized),
    next_domain_level_1_f = getEvarized[String]("next_domain_level_1_f", evarized),
    next_domain_level_2_f = getEvarized[String]("next_domain_level_2_f", evarized),
    next_domain_level_3_f = getEvarized[String]("next_domain_level_3_f", evarized),
    next_domain_level_4_f = getEvarized[String]("next_domain_level_4_f", evarized),
    next_domain_level_5_f = getEvarized[String]("next_domain_level_5_f", evarized),
    next_path_level_1_f = getEvarized[String]("next_path_level_1_f", evarized),
    next_path_level_2_f = getEvarized[String]("next_path_level_2_f", evarized),
    next_path_level_3_f = getEvarized[String]("next_path_level_3_f", evarized),
    next_path_level_4_f = getEvarized[String]("next_path_level_4_f", evarized),
    next_path_level_5_f = getEvarized[String]("next_path_level_5_f", evarized),
    next_pgn_level_1_f = getEvarized[String]("next_pgn_level_1_f", evarized),
    next_pgn_level_2_f = getEvarized[String]("next_pgn_level_2_f", evarized),
    next_pgn_level_3_f = getEvarized[String]("next_pgn_level_3_f", evarized),
    next_pgn_level_4_f = getEvarized[String]("next_pgn_level_4_f", evarized),
    next_pgn_level_5_f = getEvarized[String]("next_pgn_level_5_f", evarized),
    next_ssc_level_1_f = getEvarized[String]("next_ssc_level_1_f", evarized),
    next_ssc_level_2_f = getEvarized[String]("next_ssc_level_2_f", evarized),
    next_ssc_level_3_f = getEvarized[String]("next_ssc_level_3_f", evarized),
    next_ssc_level_4_f = getEvarized[String]("next_ssc_level_4_f", evarized),
    next_ssc_level_5_f = getEvarized[String]("next_ssc_level_5_f", evarized)
  )
}

