import Models.{CddprfRskScoreLatestFull, EsdlAccOpenDate, EsdlAccount, EsdlPartyProd, EsdlPartyXRef, EsdlTransaction, EsdlUnifiedTxn}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, coalesce, col, collect_list, collect_set, concat, concat_ws, countDistinct, dense_rank, expr, first, least, lit, month, regexp_replace, sha, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession, functions}

object TransformData {

  def removeLeadingZeros(colName: String) = regexp_replace(col(colName), "^0+", "")

  def getEsdlTxnMapping(esdlTxnDs: Dataset[EsdlTransaction], esdlAccDs: Dataset[EsdlAccount], esdlAccOpenDt: Dataset[EsdlAccOpenDate]) = {

    val esdlTxnAttrbuteMap = esdlTxnDs
      .withColumn("txn_id", concat_ws("-", col("source_system_cd"), col("txn_id")))
      .withColumn("card_number", col("card_number"))
      .withColumn("account_number2", col("account_number"))
      .withColumn("holding_branch_key2", col("holding_branch_key"))
      .withColumn("account_key", col("account_key"))
      .withColumn("source_system_cd", col("source_system_cd"))
      .withColumn("channel_cd", col("channel_cd"))
      .withColumn("source_transaction_id", col("source_transaction_id"))
      .withColumn("execution_local_date_time", col("execution_local_date_time"))
      .withColumn("posting_date", col("posting_Date"))
      .withColumn("cr_dr_code", col("cr_dr_code"))
      .withColumn("cash_code", col("cash_code"))
      .withColumn("msg_type_code", col("msg_type_code"))
      .withColumn("acct_curr_cd", col("acct_curr_cd"))
      .withColumn("acct_curr_amount", col("acct_curr_amount"))
      .withColumn("orig_curr_cd", col("orig_curr_cd"))
      .withColumn("orig_curr_amount", col("orig_curr_amount"))
      .withColumn("cad_equivalent_amt", col("cad_equivalent_amt"))
      .withColumn("orig_curr_cash_amount", col("orig_curr_cash_amount"))
      .withColumn("cash_cad_equivalent_amt", col("cash_cad_equivalent_amt"))
      .withColumn("acct_to_cad_rate", col("acct_to_cad_rate"))
      .withColumn("txn_to_acct_rate", col("txn_to_acct_rate"))
      .withColumn("txn_to_cad_rate", col("txn_to_cad_rate"))
      .withColumn("txn_curr_rate_ind", col("txn_curr_rate_ind"))
      .withColumn("ecif_composite_key3", col("ecif_composite_key"))
      .withColumn("ip_address", col("ip_address"))
      .withColumn("orph_ind", col("orph_ind"))
      .withColumn("orig_process_date", col("orig_process_date"))
      .withColumn("loaded_to_cerebro", col("loaded_to_cerebro"))
      .withColumn("loaded_to_hunter", col("loaded_to_hunter"))
      .withColumn("opp_account_number", col("opp_account_number"))
      .withColumn("opp_branch_key", col("opp_branch_key"))
      .withColumn("rules_cash_cad_equivalent_amt", col("rules_cash_cad_equivalent_amt"))
      .withColumn("operation_type", col("operation_type"))
      .withColumn("operation_type", col("operation_type"))
      .withColumn("txn_status_code", col("txn_status_code"))
      .withColumn("txn_response_cd", col("txn_response_cd"))
      .withColumn("emt_transfer_id", col("emt_transfer_id"))
      .withColumn("emt_recipient_id", col("emt_recipient_id"))
      .withColumn("recipient_sms", col("recipient_sms"))
      .withColumn("sender_email", col("sender_email"))
      .withColumn("processing_date", col("processing_date"))
      .withColumn("row_update_date", col("row_update_date"))
      .withColumn("recipient_email", col("recipient_email"))
      .withColumn("instr_agent_id", col("instr_agent_id"))
      .withColumn("instrg_agent_clearing_system", col("instrg_agent_clearing_system"))
      .withColumn("txn_status", col("txn_status"))
      .withColumn("txn_type", col("txn_type"))
      .withColumn("sndr_agt_name", col("sndr_agt_name"))
      .withColumn("utc_txn_date", col("utc_txn_date"))
      .withColumn("utc_txn_time", col("utc_txn_time"))
      .withColumn("cust1_org_legal_name", col("cust1_org_legal_name"))
      .withColumn("cust2_org_legal_name", col("cust2_org_legal_name"))
      .withColumn("cust1_bank_name", col("cust1_bank_name"))
      .withColumn("cust2_bank_name", col("cust2_bank_name"))
      .withColumn("user_id", col("user_id"))
      .withColumn("currency_conversion_rate", col("currency_conversion_rate"))
      .withColumn("user_device_type", col("user_device_type"))
      .withColumn("user_session_date_time", col("user_session_date_time"))
      .withColumn("transaction_memo_line_1", col("transaction_memo_line_1"))
      .withColumn("client_ip_addr", col("client_ip_addr"))
      .withColumn("debtor_id", col("debtor_id"))
      .withColumn("addl_field_7", col("addl_field_7"))
      .withColumn("addl_field_8", col("addl_field_8"))
      .withColumn("addl_field_9", col("addl_field_9"))
      .withColumn("addl_field_10", col("addl_field_10"))
      .withColumn("organization_unit_cd", expr("substring(instrg_agent_clearing_system, length(instrg_agent_clearing_system)-3, 4)"))
      .withColumn("opp_organization_unit_cd", expr("substring(instr_agent_id, length(instr_agent_id)-3, 4)"))
      .withColumn("fx_tran_exchange_rate", col("fx_tran_exchange_rate"))
      .drop("account_number")
      .drop("holding_branch_key")
      .drop("ecif_composite_key")


    val esdlAccDs1 = esdlAccDs.withColumn("ecif_composite_key1", esdlAccDs("ecif_composite_key")).drop(col("ecif_composite_key"))
      .withColumn("account_number1", esdlAccDs("account_number")).drop("account_number")
      .withColumn("holding_branch_key1", esdlAccDs("holding_branch_key")).drop("holding_branch_key")

    val esdlAccColMap = esdlAccDs1.
      join(esdlTxnAttrbuteMap, esdlTxnAttrbuteMap("ecif_composite_key3") === esdlAccDs1("ecif_composite_key1")
        && esdlAccDs1("account_key") === esdlTxnAttrbuteMap("account_key"), "left")
      .withColumn("product_type_code", esdlAccDs1("product_type_code"))
      .withColumn("product_cd", esdlAccDs1("product_cd"))

    val finalResult1 = esdlAccColMap.
      groupBy("opp_account_number")
      .agg(
        when(countDistinct("status_code") > 1, lit("NULL"))
          .otherwise(first("status_code"))
          .alias("customer1_account_status")
      )

    val mergedResult = esdlAccColMap
      .join(finalResult1, Seq("opp_account_number"), "left")

    val step1 = esdlTxnAttrbuteMap.alias("a")
      .join(esdlAccDs.alias("b"),
        (col("a.opp_account_number") === col("b.account_number")) &&
          (col("a.opp_branch_key").isNotNull && col("a.opp_branch_key") === col("b.holding_branch_key")) &&
          col("b.product_type_code").isin("PDEP", "DEP", "PLOA", "CL"), "left")
      .select(col("a.*"), col("b.status_code"))

    val step3 = step1.alias("a")
      .join(esdlAccDs.alias("b"),
        (removeLeadingZeros("a.opp_branch_key").isNull && !removeLeadingZeros("a.operation_type").isin("U5", "U6")) &&
          (removeLeadingZeros("a.opp_account_number") === removeLeadingZeros("b.account_number")) &&
          col("b.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP"), "left")
      .select(col("a.*"), coalesce(col("b.status_code"), col("a.status_code")).alias("status_code"))

    val lookupEcifCompositeKey = step3.alias("a")
      .join(esdlAccOpenDt.alias("e"),
        (removeLeadingZeros("a.opp_account_number") === removeLeadingZeros("e.curr_plc_acct_num")) &&
          (removeLeadingZeros("a.opp_branch_key").cast("int") === removeLeadingZeros("e.holding_branch_key_source").cast("int")) &&
          col("e.product_type_code") === "CL", "left")
      .select(col("a.*"), col("e.ecif_composite_key"))

    val finalStatusLookup = lookupEcifCompositeKey.alias("f")
      .join(esdlAccDs.alias("e"),
        removeLeadingZeros("f.ecif_composite_key") === removeLeadingZeros("e.ecif_composite_key") &&
          col("e.product_type_code") === "CL", "left")
      .select(col("f.*"), coalesce(col("e.status_code"), lit("NULL")).alias("customer2_account_status"))

    // setting to NULL if more than one found
    val finalResult = finalStatusLookup.
      groupBy("opp_account_number")
      .agg(
        when(countDistinct("customer2_account_status") > 1, lit("NULL"))
          .otherwise(first("customer2_account_status"))
          .alias("customer2_account_status")
      )
    mergedResult.join(finalResult, Seq("opp_account_number"), "left")
  }


  def getUnifiedAttMap(unifiedTxnDs: Dataset[EsdlUnifiedTxn], esdlPartyProdDs: Dataset[EsdlPartyProd], PartyXRefDs: Dataset[EsdlPartyXRef]): DataFrame = {

    val unifiedTxnDs1 = unifiedTxnDs
      .withColumn("primary_party_key", when(col("ecif_key").isNotNull, col("ecif_key")).otherwise(lit("null")))
      .withColumn("creditor_id",
        when(col("cr_dr_code").isin("D"), col("customer2_Account_holder_cif_id"))
          .when(col("cr_dr_code").isin("C"), col("customer1_Account_holder_cif_id"))
          .otherwise(lit("null")))

    val step1 = unifiedTxnDs1.alias("a")
      .join(esdlPartyProdDs.alias("b"), col("a.card_number") === removeLeadingZeros("b.account_number"), "left")
      .filter(col("b.relation_type_cd").isin(1, 3, 5, 9, 11, 31, 36, 37, 38, 40) &&
        col("b.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC"))
      .withColumn("conductor_key", col("b.party_key"))
      .drop(col("b.account_number"))
      .drop(col("b.holding_branch_key"))


    val step2 = step1.alias("s1")
      .join(PartyXRefDs.alias("c"), col("c.bus_app_id") === "344", "left")
      .withColumn("cross_ref_match", expr("substring(c.cross_ref_num, 1, 4) || '4' || substring(c.cross_ref_num, 6, length(c.cross_ref_num))"))
      .filter(col("cross_ref_match") === col("s1.account_number"))
      .withColumn("conductor_key", least(col("s1.conductor_key"), col("c.party_key")))

    val step3 = step2.alias("s2")
      .join(esdlPartyProdDs.alias("b"), removeLeadingZeros("s2.account_number") === removeLeadingZeros("b.account_number") &&
        removeLeadingZeros("s2.holding_branch_key") === removeLeadingZeros("b.holding_branch_key"), "left")
      .filter(col("b.relation_type_cd").isin(1, 3, 9) &&
        col("b.product_type_code").isin("DEP", "PDEP", "CL", "PLOA"))
      .withColumn("conductor_key", coalesce(col("s2.conductor_key"), col("b.party_key")))

    val validRelationTypes = Seq(1, 3, 5, 9, 11, 31, 36, 37, 38, 40)
    val validProductTypes = Seq("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC")

    val step4Result = step3.alias("s3")
      .join(esdlPartyProdDs.alias("b"),
        col("s3.card_number") === removeLeadingZeros("b.account_number"), "left")
      .filter(col("b.relation_type_cd").isin(validRelationTypes: _*) &&
        col("b.product_type_code").isin(validProductTypes: _*))
      .groupBy("s3.card_number")
      .agg(
        collect_list("b.party_key").alias("party_keys"),
        collect_list("b.relation_type_cd").alias("relation_types")
      )
      .withColumn("target_party_key", expr(
        """
    CASE
      WHEN size(party_keys) = 1 THEN element_at(party_keys, 1)
      WHEN array_contains(relation_types, '1') AND array_contains(relation_types, '31') THEN
        element_at(filter(party_keys, x -> array_contains(relation_types, cast(x as string))), 1)
      ELSE
        element_at(filter(party_keys, x -> !array_contains(relation_types, cast(x as string))
                                           AND !array_contains(relation_types, '31')), 1)
    END
  """))

    // conductor_key
    val finalResult = step4Result.withColumn("conductor_key", col("target_party_key")).select("conductor_key")
    finalResult

  }

  def getEcifKeyAttMap(unifiedTxnDs: Dataset[EsdlUnifiedTxn], esdlPartyProdDs: Dataset[EsdlPartyProd], esdlAccOpenDt: Dataset[EsdlAccOpenDate], cddprfRskScoreLatestfl: Dataset[CddprfRskScoreLatestFull]): DataFrame = {

    val unifiedTxnDsColRename = unifiedTxnDs
      .withColumn("product_type_code_utxn", col("product_type_code"))
      .withColumn("account_number_utxn", col("account_number"))
      .withColumn("holding_branch_key_utxn", col("holding_branch_key"))

    val esdlPartyProdColRename = esdlPartyProdDs
      .withColumn("product_type_code_prod", col("product_type_code"))
      .withColumn("account_number_prod", col("account_number"))
      .withColumn("holding_branch_key_prod", col("holding_branch_key"))
      .withColumn("ecif_composite_key_prod", col("ecif_composite_key"))
      .withColumn("party_key_prod", col("party_key"))

    val joinPartyKey = unifiedTxnDsColRename
      .alias("a").join(esdlPartyProdColRename.alias("b"),
        col("a.product_type_code_utxn") === col("b.product_type_code_prod") &&
          (col("a.account_number_utxn") === col("b.account_number_prod") || col("a.opp_account_number") === col("b.account_number_prod")) &&
          (col("a.holding_branch_key_utxn").isNotNull && col("a.holding_branch_key_utxn") === col("b.holding_branch_key_prod")) &&
          col("b.relation_type_cd") === lit("1"), "left")

    val esdlAccOpenDtColRename = esdlAccOpenDt
      .withColumn("product_type_code_accopen", col("product_type_code"))
      .withColumn("ecif_composite_key_accopen", col("ecif_composite_key"))

    val joinEcifKey = joinPartyKey.alias("a").join(esdlAccOpenDtColRename.alias("e"),
      col("a.account_number_utxn") === col("e.curr_plc_acct_num") &&
        col("a.holding_branch_key_utxn").cast("int") === col("e.holding_branch_key_source") &&
        col("e.product_type_code_accopen") === lit("CL"), "left")

    val finalPartyKey = joinEcifKey.alias("e").join(esdlPartyProdColRename.alias("b"),
        col("b.ecif_composite_key_prod") === col("e.ecif_composite_key_accopen") &&
          col("b.product_type_code_prod") === lit("CL") &&
          col("b.relation_type_cd") === lit("1"), "left")
      .drop(col("e.party_key_prod"))
      .drop(col("e.product_type_code_prod"))

    val cddprfRskScoreLatestflColRename = cddprfRskScoreLatestfl.withColumn("party_key_rsk", col("party_key"))

    val highestRiskScore = finalPartyKey.alias("b").join(cddprfRskScoreLatestflColRename.alias("c"),
        col("b.party_key_prod") === col("c.party_key_rsk") &&
          month(col("b.execution_local_date_time")) - 1 === month(col("c.fctp_process_date")), "left")
      .withColumn("rank", dense_rank().over(Window.partitionBy("b.product_type_code_prod").orderBy(col("c.party_risk_score_rv").desc)))
      .filter(col("rank") === 1)

    val finalResult = highestRiskScore.withColumn("ecif_key", when(
      col("b.product_type_code_utxn").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PPP", "PPC", "DEP", "PDEP", "CL", "PLOA"),
      null).otherwise(col("b.party_key_prod")))

    finalResult
  }

  def getCust1AccHolderCifId(esdlUnifiedTxn1: Dataset[EsdlUnifiedTxn], esdlPartyProd1: Dataset[EsdlPartyProd], esdlAccOpenDt1: Dataset[EsdlAccOpenDate]): DataFrame = {

    val joinedDF = esdlUnifiedTxn1.alias("a")
      .join(esdlPartyProd1.alias("b"),
        col("a.account_number") === col("b.account_number") &&
          col("a.holding_branch_key") === col("b.holding_branch_key") &&
          col("a.product_type_code") === col("b.product_type_code") &&
          col("b.relation_type_cd") === lit("1") &&
          col("a.product_type_code").isin("DEP", "CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "PLOA", "CL"),
        "left_outer")
      .selectExpr("a.*", "b.party_key")

    val joinedWithEcif = joinedDF.alias("a")
      .join(esdlAccOpenDt1.alias("e"),
        col("a.account_number") === col("e.curr_plc_acct_num") &&
          col("a.holding_branch_key").cast("int") === col("e.holding_branch_key_source") &&
          !col("a.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL") &&
          col("e.product_type_code") === lit("CL"),
        "left_outer")
      .selectExpr("a.*", "e.ecif_composite_key")

    val finalDF = joinedWithEcif.alias("a")
      .join(esdlPartyProd1.alias("b"),
        col("b.ecif_composite_key") === col("a.ecif_composite_key") &&
          col("b.product_type_code") === lit("CL") &&
          col("b.relation_type_cd") === lit("1"),
        "left_outer")
      .selectExpr("a.*", "b.party_key")

    val resultDF = finalDF.groupBy("account_number")
      .agg(concat_ws(";", collect_set("b.party_key")).alias("target_party_key"))
      .withColumn("customer1_account_holder_cif_id", when(col("target_party_key").isNull, lit("NULL")).otherwise(col("target_party_key")))

    resultDF.show()

    resultDF

  }

  def getAccHoldersAll(esdlUnifiedTransactions: Dataset[EsdlUnifiedTxn],
                              esdlPartyProd: Dataset[EsdlPartyProd],
                              esdlAccountOpenDate: Dataset[EsdlAccOpenDate]): DataFrame = {

    def removeLeadingZeros(col: Column): Column = regexp_replace(col, "^0+", "")

    val step1 = esdlUnifiedTransactions.as("a")
      .join(esdlPartyProd.as("b"),
        removeLeadingZeros(col("a.card_number")) === removeLeadingZeros(col("b.account_number")) &&
          col("b.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC"),
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.party_key").alias("step1_party_key")
      )

    val step2 = step1.as("a")
      .join(esdlPartyProd.as("b"),
        removeLeadingZeros(col("a.account_number")) === removeLeadingZeros(col("b.account_number")) &&
          removeLeadingZeros(col("a.holding_branch_key")) === removeLeadingZeros(col("b.holding_branch_key")) &&
          col("b.product_type_code").isin("DEP", "PDEP", "CL", "PLOA"),
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.party_key").alias("step2_party_key")
      )

    val step3a = step2.as("a")
      .filter(col("step2_party_key").isNull)
      .join(esdlAccountOpenDate.as("e"),
        removeLeadingZeros(col("a.account_number")) === removeLeadingZeros(col("e.curr_plc_acct_num")) &&
          removeLeadingZeros(col("a.holding_branch_key")).cast("int") === col("e.holding_branch_key_source") &&
          col("e.product_type_code") === "CL",
        "left_outer"
      )
      .select(
        col("a.*"),
        col("e.ecif_composite_key").alias("step3a_ecif_key")
      )

    val step3b = step3a.as("a")
      .join(esdlPartyProd.as("b"),
        col("a.step3a_ecif_key") === col("b.ecif_composite_key") &&
          col("b.product_type_code") === "CL" &&
          col("b.relation_type_cd") === "1",
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.party_key").alias("step3b_party_key")
      )

    val step2WithNull = step2
      .filter(col("step2_party_key").isNotNull)
      .withColumn("step3a_ecif_key", lit(null)) // Add missing column
      .withColumn("step3b_party_key", lit(null)) // Add missing column

    val step2And3 = step2WithNull.union(step3b)

    val step4 = step2And3.as("a")
      .join(esdlPartyProd.as("b"),
        removeLeadingZeros(col("a.opp_account_number")) === removeLeadingZeros(col("b.account_number")) &&
          removeLeadingZeros(col("a.opp_branch_key")) === removeLeadingZeros(col("b.holding_branch_key")),
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.party_key").alias("step4_party_key")
      )

    val step5a = step4.as("a")
      .filter(col("step4_party_key").isNull)
      .join(esdlAccountOpenDate.as("e"),
        removeLeadingZeros(col("a.opp_account_number")) === removeLeadingZeros(col("e.curr_plc_acct_num")) &&
          removeLeadingZeros(col("a.opp_branch_key")).cast("int") === col("e.holding_branch_key_source") &&
          col("e.product_type_code") === "C1",
        "left_outer"
      )
      .select(
        col("a.*"),
        col("e.ecif_composite_key").alias("step5a_ecif_key")
      )

    val step5b = step5a.as("a")
      .join(esdlPartyProd.as("b"),
        col("a.step5a_ecif_key") === col("b.ecif_composite_key") &&
          col("b.product_type_code") === "C1" &&
          col("b.relation_type_cd") === "1",
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.party_key").alias("step5b_party_key")
      )

    val step4WithNull = step4
      .filter(col("step4_party_key").isNotNull)
      .withColumn("step5a_ecif_key", lit(null)) // Add missing column
      .withColumn("step5b_party_key", lit(null)) // Add missing column

    val step4And5 = step4WithNull.union(step5b)

    val result = step4And5
      .withColumn("all_party_keys",
        array(
          col("step1_party_key"),
          col("step2_party_key"),
          col("step3b_party_key"),
          col("step4_party_key"),
          col("step5b_party_key"),
       //   col("conductor_key")
        ))
      .withColumn("filtered_party_keys", expr("filter(all_party_keys, x -> x is not null)"))
      .withColumn("distinct_party_keys", expr("array_distinct(filtered_party_keys)"))
      .withColumn("acct_holders_all", concat_ws(";", col("distinct_party_keys")))
      .drop("all_party_keys", "filtered_party_keys", "distinct_party_keys",
        "step1_party_key", "step2_party_key", "step3b_party_key",
        "step4_party_key", "step5b_party_key", "step3a_ecif_key", "step5a_ecif_key")

    result.show()

    result
  }

}
