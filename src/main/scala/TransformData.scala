import Models.{EsdlAccOpenDate, EsdlAccount, EsdlTransaction}
import org.apache.spark.sql.functions.{coalesce, col, concat, concat_ws, countDistinct, expr, first, lit, regexp_replace, when}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TransformData {

  def getEsdlTxnMapping(esdlTxnDs: Dataset[EsdlTransaction], esdlAccDs: Dataset[EsdlAccount], esdlAccOpenDt: Dataset[EsdlAccOpenDate])  = {

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

    def removeLeadingZeros(colName: String) = regexp_replace(col(colName), "^0+", "")

    val esdlAccDs1 = esdlAccDs.withColumn("ecif_composite_key1", esdlAccDs("ecif_composite_key")).drop(col("ecif_composite_key"))
      .withColumn("account_number1", esdlAccDs("account_number")).drop("account_number")
      .withColumn("holding_branch_key1", esdlAccDs("holding_branch_key")).drop("holding_branch_key")

    val esdlAccColMap = esdlAccDs1.
      join(esdlTxnAttrbuteMap, esdlTxnAttrbuteMap("ecif_composite_key3") === esdlAccDs1("ecif_composite_key1")
        && esdlAccDs1("account_key") === esdlTxnAttrbuteMap("account_key"), "left")
      .withColumn("product_type_code", esdlAccDs1("product_type_code"))
      .withColumn("product_cd", esdlAccDs1("product_cd"))
      .withColumn("customer1_account_status", when(esdlAccDs1("status_code").isNotNull, esdlAccDs1("status_code")).otherwise(lit("null")))
      //.drop("opp_account_number")

    val finalResult1 = esdlAccColMap.
      groupBy("opp_account_number")
      .agg(
        when(countDistinct("customer1_account_status") > 1, lit("NULL"))
          .otherwise(first("customer1_account_status"))
          .alias("customer1_account_status")
      )

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

    finalStatusLookup.show(false)

    // setting to NULL if more than one found
    val finalResult = finalStatusLookup.
      groupBy("opp_account_number")
      .agg(
        when(countDistinct("customer2_account_status") > 1, lit("NULL"))
          .otherwise(first("customer2_account_status"))
          .alias("customer2_account_status")
      )


  }
}
