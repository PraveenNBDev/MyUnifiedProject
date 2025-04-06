import Models.{CddprfRskScoreLatestFull, EsdlAccOpenDate, EsdlAccount, EsdlPartyProd, EsdlPartyXRef, EsdlTransaction, EsdlUnifiedTxn}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, array_distinct, coalesce, col, collect_list, collect_set, concat, concat_ws, countDistinct, dense_rank, element_at, expr, first, least, lit, month, regexp_replace, sha, substring, when}
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
    val mergedRes = mergedResult.join(finalResult, Seq("opp_account_number"), "left")
    mergedRes.show()
    mergedRes
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
      .withColumn("step3a_ecif_key", lit(null))
      .withColumn("step3b_party_key", lit(null))

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
          col("step5b_party_key")
        //  col("conductor_key")
        ))
      .withColumn("filtered_party_keys", expr("filter(all_party_keys, x -> x is not null)"))
      .withColumn("distinct_party_keys", expr("array_distinct(filtered_party_keys)"))
      .withColumn("acct_holders_all", concat_ws(";", col("distinct_party_keys")))
      .drop("all_party_keys", "filtered_party_keys", "distinct_party_keys",
        "step1_party_key", "step2_party_key", "step3b_party_key",
        "step4_party_key", "step5b_party_key", "step3a_ecif_key", "step5a_ecif_key")

    println("acc_holdes_all")
    result.show()

    result
  }


  def getCustomer2AccountStatus(esdlTransactions: Dataset[EsdlTransaction],
                                esdlAccount: Dataset[EsdlAccount],
                                esdlAccountOpenDate: Dataset[EsdlAccOpenDate]): DataFrame = {

    val step1Condition = col("a.opp_branch_key").isNotNull

    val esdlAccountColRename = esdlAccount.withColumn("product_type_code_acc", col("product_type_code")).drop(col("product_type_code"))

    val step1Join = esdlTransactions.as("a")
      .join(esdlAccountColRename.as("b"),
        col("a.opp_account_number") === col("b.account_number") &&
          col("a.opp_branch_key") === col("b.holding_branch_key") &&
          col("b.product_type_code_acc").isin("PDFP", "DEP", "PLOA", "CL") &&
          step1Condition,
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.status_code").as("step1_status_code")
      )

    val step3Condition = col("a.opp_branch_key").isNull &&
      !col("a.operation_type").isin("U5", "U6") &&
      col("b.product_type_code_acc").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP")


    val step3Join = step1Join.as("a")
      .join(esdlAccountColRename.as("b"),
        col("a.opp_account_number") === col("b.account_number") &&
          col("b.product_type_code_acc").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP") &&
          step3Condition,
        "left_outer"
      )
      .select(
        col("*"),
        coalesce(col("a.step1_status_code"), col("b.status_code")).as("step3_status_code")
      )

    val fallbackCondition = col("step3_status_code").isNull

    val esdlAccountOpenDateColRname = esdlAccountOpenDate.withColumn("product_type_code_2", col("product_type_code")).drop(col("product_type_code"))
    val fallbackJoin1 = step3Join.as("a")
      .join(esdlAccountOpenDateColRname.as("e"),
        col("a.opp_account_number") === col("e.curr_plc_acct_num") &&
          col("a.opp_branch_key").cast("int") === col("e.holding_branch_key_source") &&
          col("e.product_type_code_2") === "CL" &&
          fallbackCondition,
        "left_outer"
      )
      .select(
        col("a.*"),
        col("e.ecif_composite_key").as("fallback_ecif_key")
      )

    val fallbackJoin2 = fallbackJoin1.as("a")
      .join(esdlAccountColRename.as("b"),
        col("a.fallback_ecif_key") === col("b.ecif_composite_key") &&
          col("b.product_type_code_acc") === "CL",
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.status_code").as("fallback_status_code")
      )

    val resultWithStatus = fallbackJoin2
      .withColumn("all_status_codes",
        array(
          col("step1_status_code"),
          col("step3_status_code"),
          col("fallback_status_code")
        )
      )
      .withColumn("filtered_status_codes", expr("filter(all_status_codes, x -> x is not null)"))
      .withColumn("distinct_status_codes", array_distinct(col("filtered_status_codes")))

    val result = resultWithStatus
      .withColumn("customer2_account_status",
        when(functions.size(col("distinct_status_codes")) === 1,
          element_at(col("distinct_status_codes"), 1))
          .otherwise(lit(null))
      ).drop("all_status_codes", "filtered_status_codes", "distinct_status_codes",
        "step1_status_code", "step3_status_code", "fallback_status_code",
        "fallback_ecif_key")

    println("account_status")
    result.show()
    result
  }


  def getCustomer2AccountCurrencyCode(esdlTransactions: Dataset[EsdlTransaction],
                                      esdlAccount: Dataset[EsdlAccount],
                                      esdlAccountOpenDate: Dataset[EsdlAccOpenDate]): DataFrame = {

    // Step 1: Handle cases where opp_branch_key is not NULL
    val step1Results = esdlTransactions.as("a")
      .filter(col("a.opp_branch_key").isNotNull)
      .join(esdlAccount.as("b"),
        col("a.opp_account_number") === col("b.account_number") &&
          col("a.opp_branch_key") === col("b.holding_branch_key") &&
          col("b.product_type_code").isin("PDP", "DEP", "PLOA", "CL"),
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.acct_curr_cd").as("found_currency_code"),
        lit("step1").as("source_step")
      )

    // Step 3: Handle cases where opp_branch_key is NULL for specific product types
    val step3Results = esdlTransactions.as("a")
      .filter(col("a.opp_branch_key").isNull)
      .join(esdlAccount.as("b"),
        col("a.opp_account_number") === col("b.account_number") &&
          col("b.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP"),
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.acct_curr_cd").as("found_currency_code"),
        lit("step3").as("source_step")
      )

    // Combine results from step1 and step3
    val combinedResults = step1Results.unionByName(step3Results)

    println("combinedResults")
    combinedResults.show()

    // Identify records that didn't get any currency code from steps 1 or 3
    val unmatchedRecords = esdlTransactions.as("a")
      .join(combinedResults.select("opp_account_number", "opp_branch_key"),
        Seq("opp_account_number", "opp_branch_key"),
        "left_anti"
      )

    // Step 4: Fallback logic for unmatched records with OLB source system
    val fallbackResults = unmatchedRecords.as("a")
      .filter(col("a.source_system_cd") === "OLB")
      .join(esdlAccountOpenDate.as("e"),
        col("a.opp_account_number") === col("e.curr_plc_acct_num") &&
          col("a.opp_branch_key").cast("int") === col("e.holding_branch_key_source") &&
          col("e.product_type_code") === "CL",
        "left_outer"
      )
      .join(esdlAccount.as("b"),
        col("e.ecif_composite_key") === col("b.ecif_composite_key") &&
          col("b.product_type_code") === "CL",
        "left_outer"
      )
      .select(
        col("a.*"),
        col("b.acct_curr_cd").as("found_currency_code"),
        lit("step4").as("source_step")
      )

    // Combine all results
    val allResults = combinedResults.unionByName(fallbackResults)

    // Handle multiple possible currency codes for the same record
    val currencyAggregation = allResults
      .groupBy("opp_account_number", "opp_branch_key")
      .agg(
        collect_set("found_currency_code").as("currency_codes"),
        collect_set("source_step").as("source_steps")
      )
      .withColumn("currency_code_count", functions.size(col("currency_codes")))
      .withColumn("distinct_currency_codes",
        when(col("currency_code_count") > 0, col("currency_codes")).otherwise(array())
      )

    // Apply business rules for final currency determination
    val finalResults = currencyAggregation
      .withColumn("customer2_account_currency_code",
        when(col("currency_code_count") === 1, element_at(col("currency_codes"), 1))
          .otherwise(lit(null)) // NULL if multiple currency codes found
      )
      .drop("currency_codes", "source_steps", "currency_code_count", "distinct_currency_codes")

    // Join back with original transactions to get all fields
    esdlTransactions.join(finalResults,
      Seq("opp_account_number", "opp_branch_key"),
      "left_outer"
    )

    println("account_currency_code")
    finalResults.show()
    finalResults
  }

  def calculateOppProdType(
                            esdlPartyProd: Dataset[EsdlPartyProd],
                            esdlTransactions: Dataset[EsdlTransaction],
                            esdlAccountOpenDate: Dataset[EsdlAccOpenDate]
                          ): DataFrame = {

    // First join condition: when opp_branch_key is not null
    val joinCondition1 = when(col("b.opp_branch_key").isNotNull,
      col("b.opp_account_number") === col("a.account_number") &&
        col("a.product_type_code").isin("DEP", "PDFP", "CARD", "CC", "PCFC", "SVSA", "VISA", "PLOA") &&
        col("b.opp_branch_key").cast("int") === col("a.holding_branch_key")
    ).otherwise(false)

    // Second join condition: when opp_branch_key is null (first case)
    val joinCondition2 = when(col("b.opp_branch_key").isNull,
      col("b.opp_account_number") === cleanAccountNumber(col("a.account_number")) &&
        col("a.product_type_code").isin("DEP", "PDFP", "CARD", "CC", "PCFC", "SVSA", "VISA", "PLOA")
    ).otherwise(false)

    // Third join condition: when opp_branch_key is null (second case)
    val joinCondition3 = when(col("b.opp_branch_key").isNull,
      substring(col("b.opp_account_number"), -7, 7) === col("a.account_number") &&
        substring(col("b.opp_account_number"), 1, 5) === col("a.holding_branch_key") &&
        col("a.product_type_code").isin("DEP", "CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "CL")
    ).otherwise(false)

    // Fourth join condition: when opp_branch_key is null (third case)
    val joinCondition4 = when(col("b.opp_branch_key").isNull,
      substring(col("b.opp_account_number"), -12, 12) === col("a.account_number") &&
        substring(col("b.opp_account_number"), 1, 5) === col("a.holding_branch_key") &&
        col("a.product_type_code").isin("PDFP", "PLOA")
    ).otherwise(false)

    // Join esdl_party_prod with esdl_transactions using all conditions
    val joinedWithPartyProd = esdlTransactions.alias("b")
      .join(esdlPartyProd.alias("a"),
        joinCondition1 || joinCondition2 || joinCondition3 || joinCondition4,
        "left_outer")
      .select(
        col("b.*"),
        col("a.product_type_code").alias("party_prod_type_code")
      )

    // For each transaction, collect all matching product_type_codes and handle ambiguity
    val withDistinctProdTypes = joinedWithPartyProd
      .groupBy("b.opp_account_number", "b.opp_branch_key")
      .agg(
        collect_set("party_prod_type_code").alias("prod_type_codes")
      )
      .withColumn("distinct_prod_type",
        when(functions.size(col("prod_type_codes")) === 1, element_at(col("prod_type_codes"), 1))
          .otherwise(lit(null))
      )

    // Join back to get all transaction fields
    val withPartyProdResult = esdlTransactions.alias("b")
      .join(withDistinctProdTypes.alias("d"),
        col("b.opp_account_number") === col("d.opp_account_number") &&
          col("b.opp_branch_key") === col("d.opp_branch_key"),
        "left_outer"
      )
      .select(
        col("b.*"),
        col("d.distinct_prod_type").alias("party_prod_type_result")
      )

    // Now handle the CLASS/PLC (Loan) account case
    val withLoanCheck = withPartyProdResult.alias("b")
      .join(esdlAccountOpenDate.alias("c"),
        col("b.opp_account_number") === col("c.curr_plc_acct_num") &&
          col("b.opp_branch_key").cast("int") === col("c.holding_branch_key_source") &&
          col("c.product_type_code") === "CL",
        "left_outer"
      )
      .select(
        col("b.*"),
        col("c.product_type_code").alias("loan_prod_type")
      )

    // Final logic to determine opp_prod_type
    val result = withLoanCheck
      .withColumn("opp_prod_type",
        when(col("party_prod_type_result").isNotNull, col("party_prod_type_result"))
          .when(col("loan_prod_type").isNotNull, col("loan_prod_type"))
          .otherwise(lit(null))
      )
      .drop("party_prod_type_result", "loan_prod_type")

    println("opp_prod_type")
    result.show()

    result
  }

  // Helper function to clean account numbers by dropping leading zeros
  def cleanAccountNumber(accountNumber: Column): Column = {
    regexp_replace(accountNumber, "^0+", "")
  }

}
