import Models.{CddprfRskScoreLatestFull, EsdlAccOpenDate, EsdlAccount, EsdlPartyProd, EsdlPartyXRef, EsdlTransaction, EsdlUnifiedTxn}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, array_contains, array_distinct, coalesce, col, collect_list, collect_set, concat, concat_ws, countDistinct, dense_rank, element_at, expr, first, least, lit, month, regexp_replace, sha, substring, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession, functions}

object TransformData {

  def getEsdlTxnMapping(esdlTxnDs: Dataset[EsdlTransaction],
                        esdlAccDs: Dataset[EsdlAccount],
                        esdlAccOpenDt: Dataset[EsdlAccOpenDate]): DataFrame = {

    // function to remove leading zeros
    def removeLeadingZeros(colName: String): Column = regexp_replace(col(colName), "^0+", "")

    val esdlTxnAttrbuteMap = esdlTxnDs.select(
        col("txn_id"),
        col("source_system_cd"),
        col("account_number"),
        col("holding_branch_key"),
        col("account_key"),
        col("ecif_composite_key"),
        col("card_number").alias("card_number21"),
        col("channel_cd"),
        col("source_transaction_id"),
        col("fx_rate_exchange_rate"),
        col("rules_cad_equivalent_amt"),
        col("execution_local_date_time"),
        col("posting_Date").as("posting_date"),
        col("cr_dr_code"),
        col("cash_code"),
        col("msg_type_code"),
        col("acct_curr_cd"),
        col("acct_curr_amount"),
        col("orig_curr_cd"),
        col("orig_curr_amount"),
        col("cad_equivalent_amt"),
        col("orig_curr_cash_amount"),
        col("cash_cad_equivalent_amt"),
        col("acct_to_cad_rate"),
        col("txn_to_acct_rate"),
        col("txn_to_cad_rate"),
        col("txn_curr_rate_ind"),
        col("ip_address"),
        col("orph_ind"),
        col("orig_process_date"),
        col("loaded_to_cerebro"),
        col("loaded_to_hunter"),
        col("opp_account_number"),
        col("opp_branch_key"),
        col("rules_cash_cad_equivalent_amt"),
        col("operation_type"),
        col("txn_status_code"),
        col("txn_response_cd"),
        col("emt_transfer_id"),
        col("emt_recipient_id"),
        col("recipient_sms"),
        col("sender_email"),
        col("processing_date"),
        col("row_update_date"),
        col("recipient_email"),
        col("instr_agent_id"),
        col("instrg_agent_clearing_system"),
        col("txn_status"),
        col("txn_type"),
        col("sndr_agt_name"),
        col("utc_txn_date"),
        col("utc_txn_time"),
        col("cust1_org_legal_name"),
        col("cust2_org_legal_name"),
        col("cust1_bank_name"),
        col("cust2_bank_name"),
        col("user_id"),
        col("currency_conversion_rate"),
        col("user_device_type"),
        col("user_session_date_time"),
        col("transaction_memo_line_1"),
        col("client_ip_addr"),
        col("debtor_id"),
        col("addl_field_7"),
        col("addl_field_8"),
        col("addl_field_9"),
        col("addl_field_10"),
        col("fx_tran_exchange_rate")
      )
      .withColumn("txn_id1", concat_ws("-", col("source_system_cd"), col("txn_id")))
      .withColumnRenamed("account_number", "account_number2")
      .withColumnRenamed("holding_branch_key", "holding_branch_key2")
      .withColumnRenamed("ecif_composite_key", "ecif_composite_key3")
      .withColumn("organization_unit_cd",
        expr("substring(instrg_agent_clearing_system, length(instrg_agent_clearing_system)-3, 4)"))
      .withColumn("opp_organization_unit_cd",
        expr("substring(instr_agent_id, length(instr_agent_id)-3, 4)"))

    // Step 2: Transform account data with explicit column selection
    val esdlAccDs1 = esdlAccDs.select(
        col("ecif_composite_key"),
        col("account_number"),
        col("holding_branch_key"),
        col("account_key"),
        col("product_type_code"),
        col("product_cd"),
        col("status_code")
      )
      .withColumnRenamed("ecif_composite_key", "ecif_composite_key1")
      .withColumnRenamed("account_number", "account_number1")
      .withColumnRenamed("holding_branch_key", "holding_branch_key1")

    // Step 3: Join transaction and account data with explicit column selection
    val esdlAccColMap = esdlAccDs1.join(
        esdlTxnAttrbuteMap,
        esdlTxnAttrbuteMap("ecif_composite_key3") === esdlAccDs1("ecif_composite_key1") &&
          esdlAccDs1("account_key") === esdlTxnAttrbuteMap("account_key"),
        "left"
      )
      .select(
        esdlTxnAttrbuteMap("*"),
        esdlAccDs1("product_type_code"),
        esdlAccDs1("product_cd"),
        esdlAccDs1("status_code")
      )

    // Step 4: Calculate customer1 account status
    val customer1Status = esdlAccColMap
      .groupBy("opp_account_number")
      .agg(
        when(countDistinct("status_code") > 1, lit("NULL"))
          .otherwise(first("status_code"))
          .alias("customer1_account_status")
      )

    // Step 5: First join for customer2 status (when opp_branch_key is not null)
    val step1 = esdlTxnAttrbuteMap.alias("txn")
      .join(
        esdlAccDs.select(
          col("account_number"),
          col("holding_branch_key"),
          col("status_code"),
          col("product_type_code")
        ).alias("acc"),
        col("txn.opp_account_number") === col("acc.account_number") &&
          col("txn.opp_branch_key").isNotNull &&
          col("txn.opp_branch_key") === col("acc.holding_branch_key") &&
          col("acc.product_type_code").isin("PDEP", "DEP", "PLOA", "CL"),
        "left"
      )
      .select(
        esdlTxnAttrbuteMap("*"),
        col("acc.status_code").alias("status_code_from_acc")
      )

    // Step 6: Second join for customer2 status (when opp_branch_key is null)
    val step3 = step1.alias("txn")
      .join(
        esdlAccDs.select(
          col("account_number"),
          col("status_code"),
          col("product_type_code")
        ).alias("acc"),
        removeLeadingZeros("txn.opp_branch_key").isNull &&
          !removeLeadingZeros("txn.operation_type").isin("U5", "U6") &&
          removeLeadingZeros("txn.opp_account_number") === removeLeadingZeros("acc.account_number") &&
          col("acc.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP"),
        "left"
      )
      .select(
        step1("*"),
        coalesce(col("acc.status_code"), col("txn.status_code_from_acc")).alias("status_code_combined")
      )

    // Step 7: Lookup ECIF composite key for loan accounts
    val lookupEcifCompositeKey = step3.alias("txn")
      .join(
        esdlAccOpenDt.select(
          col("curr_plc_acct_num"),
          col("holding_branch_key_source"),
          col("ecif_composite_key"),
          col("product_type_code")
        ).alias("acc_open"),
        removeLeadingZeros("txn.opp_account_number") === removeLeadingZeros("acc_open.curr_plc_acct_num") &&
          removeLeadingZeros("txn.opp_branch_key").cast("int") === removeLeadingZeros("acc_open.holding_branch_key_source").cast("int") &&
          col("acc_open.product_type_code") === "CL",
        "left"
      )
      .select(
        step3("*"),
        col("acc_open.ecif_composite_key").alias("ecif_key_from_acc_open")
      )

    // Step 8: Final status lookup for customer2
    val finalStatusLookup = lookupEcifCompositeKey.alias("txn")
      .join(
        esdlAccDs.select(
          col("ecif_composite_key"),
          col("status_code"),
          col("product_type_code")
        ).alias("acc"),
        removeLeadingZeros("txn.ecif_key_from_acc_open") === removeLeadingZeros("acc.ecif_composite_key") &&
          col("acc.product_type_code") === "CL",
        "left"
      )
      .select(
        lookupEcifCompositeKey("*"),
        coalesce(col("acc.status_code"), lit("NULL")).alias("customer2_account_status_temp")
      )

    // Step 9: Aggregate customer2 status to handle duplicates
    val customer2Status = finalStatusLookup
      .groupBy("opp_account_number")
      .agg(
        when(countDistinct("customer2_account_status_temp") > 1, lit("NULL"))
          .otherwise(first("customer2_account_status_temp"))
          .alias("customer2_account_status1")
      )

    // Step 10: Combine all results
    val finalRes = esdlAccColMap
      .join(customer1Status, Seq("opp_account_number"), "left")
      .join(customer2Status, Seq("opp_account_number"), "left")

    finalRes
  }

  def getUnifiedAttMap(
                        unifiedTxnDs: Dataset[EsdlUnifiedTxn],
                        esdlPartyProdDs: Dataset[EsdlPartyProd],
                        partyXRefDs: Dataset[EsdlPartyXRef]
                      ): DataFrame = {

    // Helper function to remove leading zeros
    def removeLeadingZeros(accountNumber: Column): Column = {regexp_replace(accountNumber, "^0+", "")}

    // Define valid relation and product types
    val validRelationTypes = Seq(1, 3, 5, 9, 11, 31, 36, 37, 38, 40)
    val validProductTypes = Seq("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC")

    // Step 1: Prepare unified transaction data with primary keys and creditor ID
    val unifiedTxnDs1 = unifiedTxnDs.select(
     // col("txn_id"),
      col("account_number"),
      col("holding_branch_key"),
      col("card_number"),
      col("cr_dr_code"),
      col("ecif_key"),
      col("customer1_Account_holder_cif_id"),
      col("customer2_Account_holder_cif_id")
    ).withColumn(
      "primary_party_key",
      when(col("ecif_key").isNotNull, col("ecif_key")).otherwise(lit("null"))
    ).withColumn(
      "creditor_id",
      when(col("cr_dr_code").isin("D"), col("customer2_Account_holder_cif_id"))
        .when(col("cr_dr_code").isin("C"), col("customer1_Account_holder_cif_id"))
        .otherwise(lit("null"))
    )

    // Step 2: Join with party product data for card transactions
    val step1 = unifiedTxnDs1.alias("txn")
      .join(
        esdlPartyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("holding_branch_key").alias("party_branch_key"),
          col("party_key"),
          col("relation_type_cd"),
          col("product_type_code")
        ).alias("party"),
        col("txn.card_number") === removeLeadingZeros(col("party.party_account_number")),
        "left"
      )
      .filter(
        col("party.relation_type_cd").isin(validRelationTypes: _*) &&
          col("party.product_type_code").isin(validProductTypes: _*)
      )
      .select(
        col("txn.*"),
        col("party.party_key").alias("conductor_key"),
        when(col("party.relation_type_cd").isin("11", "40"), "Y").otherwise("N").alias("on_behalf_of_ind")
      )
      .withColumn(
        "third_party_cif_id",
        when(col("on_behalf_of_ind") === "Y", col("customer1_Account_holder_cif_id")).otherwise("null")
    )

    // Step 3: Join for joint account indicator
    val step1_1 = unifiedTxnDs.select(
        col("account_number"),
        col("holding_branch_key"),
        col("card_number")
      ).alias("txn")
      .join(
        esdlPartyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("holding_branch_key").alias("party_branch_key"),
          col("owner_count")
        ).alias("party"),
        col("txn.account_number") === col("party.party_account_number") &&
          col("txn.holding_branch_key") === col("party.party_branch_key"),
        "left"
      )
      .select(
        col("txn.*"),
        when(col("party.owner_count") > 1, "Y")
          .when(col("party.owner_count") === 1, "N")
          .otherwise(null)
          .alias("jnt_act_holder")
      )

    // Step 4: Combine results from step1 and step1_1
    val combinedSteps = step1.unionByName(step1_1, allowMissingColumns = true)

    // Step 5: Join with party cross reference data
    val step2 = combinedSteps.alias("txn")
      .join(
        partyXRefDs.select(
          col("cross_ref_num"),
          col("party_key").alias("xref_party_key"),
          col("bus_app_id")
        ).alias("xref"),
        col("xref.bus_app_id") === "344",
        "left"
      )
      .withColumn(
        "cross_ref_match",
        expr("substring(xref.cross_ref_num, 1, 4) || '4' || substring(xref.cross_ref_num, 6, length(xref.cross_ref_num))")
      )
      .filter(col("cross_ref_match") === col("txn.account_number"))
      .withColumn(
        "conductor_key",
        least(col("txn.conductor_key"), col("xref.xref_party_key"))
      )

    // Step 6: Final join with party product data for account transactions
    val step3 = step2.alias("txn")
      .join(
        esdlPartyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("holding_branch_key").alias("party_branch_key"),
          col("party_key"),
          col("relation_type_cd"),
          col("product_type_code")
        ).alias("party"),
        removeLeadingZeros(col("txn.account_number")) === removeLeadingZeros(col("party.party_account_number")) &&
          removeLeadingZeros(col("txn.holding_branch_key")) === removeLeadingZeros(col("party.party_branch_key")),
        "left"
      )
      .filter(
        col("party.relation_type_cd").isin(1, 3, 9) &&
          col("party.product_type_code").isin("DEP", "PDEP", "CL", "PLOA")
      )
      .withColumn(
        "conductor_key",
        coalesce(col("txn.conductor_key"), col("party.party_key"))
      )

    // Step 7: Final processing to determine target party key
    val windowSpec = Window.partitionBy("txn.account_number")

    val finalResult = step3.alias("txn")
      .join(
        esdlPartyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("party_key"),
          col("relation_type_cd"),
          col("product_type_code")
        ).alias("party"),
        col("txn.card_number") === removeLeadingZeros(col("party.party_account_number")),
        "left"
      )
      .filter(
        col("party.relation_type_cd").isin(validRelationTypes: _*) &&
          col("party.product_type_code").isin(validProductTypes: _*)
      )
      .withColumn(
        "party_keys",
        collect_list(col("party.party_key")).over(windowSpec))
      .withColumn(
        "relation_types",
        collect_list(col("party.relation_type_cd")).over(windowSpec))
      .withColumn(
        "target_party_key",
        when(functions.size(col("party_keys")) === 1, element_at(col("party_keys"), 1))
          .when(
            array_contains(col("relation_types"), "1") &&
              array_contains(col("relation_types"), "31"),
            element_at(col("party_keys"), 1)
          )
          .otherwise(element_at(col("party_keys"), 1))
      )
      .withColumn("conductor_key", col("target_party_key"))
      .select(
       // col("txn.txn_id"),
        col("txn.party_account_number"),
        col("conductor_key"),
        col("on_behalf_of_ind"),
        col("third_party_cif_id"),
        col("jnt_act_holder"),
        col("primary_party_key"),
        col("creditor_id")
        // Include other needed columns
      )

    finalResult
  }

  def getEcifKeyAttMap(
                        unifiedTxnDs: Dataset[EsdlUnifiedTxn],
                        esdlPartyProdDs: Dataset[EsdlPartyProd],
                        esdlAccOpenDt: Dataset[EsdlAccOpenDate],
                        cddprfRskScoreLatestfl: Dataset[CddprfRskScoreLatestFull]
                      ): DataFrame = {

    // Step 1: Select and rename columns from unified transactions
    val unifiedTxnCols = unifiedTxnDs.select(
      col("product_type_code").alias("product_type_code_utxn"),
      col("account_number").alias("account_number_utxn"),
      col("holding_branch_key").alias("holding_branch_key_utxn"),
      col("opp_account_number"),
      col("execution_local_date_time")
    )

    // Step 2: Select and rename columns from party product data
    val partyProdCols = esdlPartyProdDs.select(
      col("product_type_code").alias("product_type_code_prod"),
      col("account_number").alias("account_number_prod"),
      col("holding_branch_key").alias("holding_branch_key_prod"),
      col("ecif_composite_key").alias("ecif_composite_key_prod"),
      col("party_key").alias("party_key_prod"),
      col("relation_type_cd")
    )

    // Step 3: Join unified transactions with party product data
    val joinPartyKey = unifiedTxnCols.alias("txn")
      .join(
        partyProdCols.alias("party"),
        col("txn.product_type_code_utxn") === col("party.product_type_code_prod") &&
          (col("txn.account_number_utxn") === col("party.account_number_prod") ||
            col("txn.opp_account_number") === col("party.account_number_prod")) &&
          col("txn.holding_branch_key_utxn").isNotNull &&
          col("txn.holding_branch_key_utxn") === col("party.holding_branch_key_prod") &&
          col("party.relation_type_cd") === lit("1"),
        "left"
      )
      .select(
        col("txn.*"),
        col("party.party_key_prod"),
        col("party.ecif_composite_key_prod")
      )

    // Step 4: Select and rename columns from account open date
    val accOpenCols = esdlAccOpenDt.select(
      col("curr_plc_acct_num"),
      col("holding_branch_key_source"),
      col("ecif_composite_key").alias("ecif_composite_key_accopen"),
      col("product_type_code").alias("product_type_code_accopen")
    )

    // Step 5: Join with account open date data
    val joinEcifKey = joinPartyKey.alias("txn")
      .join(
        accOpenCols.alias("acc_open"),
        col("txn.account_number_utxn") === col("acc_open.curr_plc_acct_num") &&
          col("txn.holding_branch_key_utxn").cast("int") === col("acc_open.holding_branch_key_source") &&
          col("acc_open.product_type_code_accopen") === lit("CL"),
        "left"
      )
      .select(
        col("txn.*"),
        col("acc_open.ecif_composite_key_accopen")
      )

    // Step 6: Final join with party product data for CL accounts
    val finalPartyKey = joinEcifKey.alias("txn")
      .join(
        partyProdCols.alias("party_cl"),
        col("party_cl.ecif_composite_key_prod") === col("txn.ecif_composite_key_accopen") &&
          col("party_cl.product_type_code_prod") === lit("CL") &&
          col("party_cl.relation_type_cd") === lit("1"),
        "left"
      )
      .select(
        col("txn.product_type_code_utxn"),
        col("txn.account_number_utxn"),
        col("txn.holding_branch_key_utxn"),
        col("txn.execution_local_date_time"),
        col("party_cl.party_key_prod").alias("cl_party_key"),
        col("txn.party_key_prod").alias("original_party_key")
      )

    // Step 7: Select and rename columns from risk score data
    val riskScoreCols = cddprfRskScoreLatestfl.select(
      col("party_key").alias("party_key_rsk"),
      col("party_risk_score_rv"),
      col("fctp_process_date")
    )

    val windowSpec = Window.partitionBy("product_type_code_utxn").orderBy(col("party_risk_score_rv").desc)

    val highestRiskScore = finalPartyKey.alias("txn")
      .join(
        riskScoreCols.alias("risk"),
        col("txn.cl_party_key") === col("risk.party_key_rsk") &&
          month(col("txn.execution_local_date_time")) - 1 === month(col("risk.fctp_process_date")),
        "left"
      )
      .withColumn("rank", dense_rank().over(windowSpec))
      .filter(col("rank") === 1)
      .select(
        col("txn.*"),
        col("risk.party_risk_score_rv")
      )

    // Step 9: Determine final ECIF key
    val excludedProductTypes = Seq("CARD", "CC", "PCFC", "SVSA", "VISA", "PPP", "PPC", "DEP", "PDEP", "CL", "PLOA")

    highestRiskScore.withColumn(
      "ecif_key",
      when(
        col("product_type_code_utxn").isin(excludedProductTypes: _*),
        lit(null)
      ).otherwise(col("cl_party_key")))
        .select(
         // col("txn_id"),
          col("ecif_key"),
          col("party_risk_score_rv"),
          col("account_number_utxn")
          // Include other needed columns
        )
  }

  def getCust1AccHolderCifId(
                              unifiedTxnDs: Dataset[EsdlUnifiedTxn],
                              partyProdDs: Dataset[EsdlPartyProd],
                              accOpenDtDs: Dataset[EsdlAccOpenDate]
                            ): DataFrame = {

    val validProductTypes = Seq("DEP", "CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "PLOA", "CL")

    val unifiedTxnCols = unifiedTxnDs.select(
      col("account_number"),
      col("holding_branch_key"),
      col("product_type_code")
    )

    val partyProdCols = partyProdDs.select(
      col("account_number").alias("party_account_number"),
      col("holding_branch_key").alias("party_branch_key"),
      col("product_type_code").alias("party_product_type"),
      col("party_key"),
      col("relation_type_cd"),
      col("ecif_composite_key")
    )

    // Step 3 Join unified transactions with party product data
    val accountPartyJoin = unifiedTxnCols.alias("txn")
      .join(
        partyProdCols.alias("party"),
        col("txn.account_number") === col("party.party_account_number") &&
          col("txn.holding_branch_key") === col("party.party_branch_key") &&
          col("txn.product_type_code") === col("party.party_product_type") &&
          col("party.relation_type_cd") === lit("1") &&
          col("txn.product_type_code").isin(validProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.account_number"),
        col("txn.holding_branch_key"),
        col("txn.product_type_code"),
        col("party.party_key")
      )

    // Step 4 Select needed columns from account open date
    val accOpenCols = accOpenDtDs.select(
      col("curr_plc_acct_num"),
      col("holding_branch_key_source"),
      col("ecif_composite_key"),
      col("product_type_code")
    )

    // Define excluded product types for ECIF lookup
    val excludedProductTypes = Seq("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL")

    // Step 5 Join with account open date for non-excluded product types
    val ecifLookupJoin = accountPartyJoin.alias("txn")
      .join(
        accOpenCols.alias("acc_open"),
        col("txn.account_number") === col("acc_open.curr_plc_acct_num") &&
          col("txn.holding_branch_key").cast("int") === col("acc_open.holding_branch_key_source") &&
          !col("txn.product_type_code").isin(excludedProductTypes: _*) &&
          col("acc_open.product_type_code") === lit("CL"),
        "left_outer"
      )
      .select(
        col("txn.*"),
        col("acc_open.ecif_composite_key")
      )

    // Step 6: Final join with party product data for CL accounts
    val finalJoin = ecifLookupJoin.alias("txn")
      .join(
        partyProdCols.alias("party_cl"),
        col("party_cl.ecif_composite_key") === col("txn.ecif_composite_key") &&
          col("party_cl.party_product_type") === lit("CL") &&
          col("party_cl.relation_type_cd") === lit("1"),
        "left_outer"
      )
      .select(
        col("txn.account_number"),
        col("txn.party_key").alias("initial_party_key"),
        col("party_cl.party_key").alias("cl_party_key")
      )

    // Step 7: Combine party keys and create final CIF ID
    finalJoin
      .withColumn(
        "combined_party_key",
        when(col("initial_party_key").isNotNull, col("initial_party_key"))
          .otherwise(col("cl_party_key"))
      )
      .groupBy("account_number")
      .agg(
        concat_ws(";", collect_set("combined_party_key")).alias("customer1_account_holder_cif_id")
      )
      .withColumn(
        "customer1_account_holder_cif_id",
        when(col("customer1_account_holder_cif_id").isNull, lit("NULL"))
          .otherwise(col("customer1_account_holder_cif_id"))
      )
  }

  def getAccHoldersAll(
                        unifiedTxnDs: Dataset[EsdlUnifiedTxn],
                        partyProdDs: Dataset[EsdlPartyProd],
                        accOpenDtDs: Dataset[EsdlAccOpenDate]
                      ): DataFrame = {

    // Helper function to remove leading zeros
    def removeLeadingZeros(col: Column): Column = regexp_replace(col, "^0+", "")

    // Define product type constants
    val cardProductTypes = Seq("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC")
    val accountProductTypes = Seq("DEP", "PDEP", "CL", "PLOA")

    // Step 1: Join with party product data for card transactions
    val step1 = unifiedTxnDs.select(
       // col("txn_id"),
        col("account_number"),
        col("holding_branch_key"),
        col("card_number"),
        col("opp_account_number"),
        col("opp_branch_key")
      ).alias("txn")
      .join(
        partyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("product_type_code"),
          col("party_key"),
          col("relation_type_cd")
        ).alias("party_card"),
        removeLeadingZeros(col("txn.card_number")) === removeLeadingZeros(col("party_card.party_account_number")) &&
          col("party_card.product_type_code").isin(cardProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.*"),
        col("party_card.party_key").alias("card_party_key")
      )

    // Step 2: Join with party product data for account transactions
    val step2 = step1.alias("txn")
      .join(
        partyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("holding_branch_key").alias("party_branch_key"),
          col("product_type_code"),
          col("party_key"),
          col("relation_type_cd")
        ).alias("party_account"),
        removeLeadingZeros(col("txn.account_number")) === removeLeadingZeros(col("party_account.party_account_number")) &&
          removeLeadingZeros(col("txn.holding_branch_key")) === removeLeadingZeros(col("party_account.party_branch_key")) &&
          col("party_account.product_type_code").isin(accountProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.*"),
        //col("txn.card_party_key"),
        col("party_account.party_key").alias("account_party_key")
      )

    // Step 3: Handle cases where account party key is null (CL account lookup)
    val step3a = step2.filter(col("account_party_key").isNull)
      .join(
        accOpenDtDs.select(
          col("curr_plc_acct_num"),
          col("holding_branch_key_source"),
          col("ecif_composite_key"),
          col("product_type_code")
        ).alias("acc_open"),
        removeLeadingZeros(col("account_number")) === removeLeadingZeros(col("acc_open.curr_plc_acct_num")) &&
          removeLeadingZeros(col("holding_branch_key")).cast("int") === col("acc_open.holding_branch_key_source") &&
          col("acc_open.product_type_code") === "CL",
        "left_outer"
      )
      .select(
       // col("txn_id"),
        col("account_number"),
        col("holding_branch_key"),
        col("card_number"),
        col("opp_account_number"),
        col("opp_branch_key"),
        col("card_party_key"),
        col("account_party_key"),
        col("acc_open.ecif_composite_key").alias("cl_ecif_key")
      )

    val step3b = step3a.alias("txn")
      .join(
        partyProdDs.select(
          col("ecif_composite_key"),
          col("party_key"),
          col("product_type_code"),
          col("relation_type_cd")
        ).alias("party_cl"),
        col("txn.cl_ecif_key") === col("party_cl.ecif_composite_key") &&
          col("party_cl.product_type_code") === "CL" &&
          col("party_cl.relation_type_cd") === "1",
        "left_outer"
      )
      .select(
        col("txn.*"),
        col("party_cl.party_key").alias("cl_party_key")
      )

    // Combine steps 2 and 3
    val step2WithAccountKey = step2.filter(col("account_party_key").isNotNull)
      .withColumn("cl_party_key", lit(null))
      .withColumn("cl_ecif_key", lit(null))

    val step2And3 = step2WithAccountKey.unionByName(step3b)

    // Step 4: Join with party product data for counterparty accounts
    val step4 = step2And3.alias("txn")
      .join(
        partyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("holding_branch_key").alias("party_branch_key"),
          col("party_key")
        ).alias("party_counterparty"),
        removeLeadingZeros(col("txn.opp_account_number")) === removeLeadingZeros(col("party_counterparty.party_account_number")) &&
          removeLeadingZeros(col("txn.opp_branch_key")) === removeLeadingZeros(col("party_counterparty.party_branch_key")),
        "left_outer"
      )
      .select(
        col("txn.*"),
        col("party_counterparty.party_key").alias("counterparty_party_key")
      )

    // Step 5: Handle cases where counterparty party key is null (C1 account lookup)
    val step5a = step4.filter(col("counterparty_party_key").isNull)
      .join(
        accOpenDtDs.select(
          col("curr_plc_acct_num"),
          col("holding_branch_key_source"),
          col("ecif_composite_key"),
          col("product_type_code")
        ).alias("acc_open_c1"),
        removeLeadingZeros(col("opp_account_number")) === removeLeadingZeros(col("acc_open_c1.curr_plc_acct_num")) &&
          removeLeadingZeros(col("opp_branch_key")).cast("int") === col("acc_open_c1.holding_branch_key_source") &&
          col("acc_open_c1.product_type_code") === "C1",
        "left_outer"
      )
      .select(
        col("txn.*"),
        col("acc_open_c1.ecif_composite_key").alias("c1_ecif_key"),
        col("counterparty_party_key")
      )

    val step5b = step5a.alias("txn")
      .join(
        partyProdDs.select(
          col("ecif_composite_key"),
          col("party_key"),
          col("product_type_code"),
          col("relation_type_cd")
        ).alias("party_c1"),
        col("txn.c1_ecif_key") === col("party_c1.ecif_composite_key") &&
          col("party_c1.product_type_code") === "C1" &&
          col("party_c1.relation_type_cd") === "1",
        "left_outer"
      )
      .select(
        col("txn.*"),
        col("party_c1.party_key").alias("c1_party_key"),
      )

    // Combine steps 4 and 5
    val step4WithCounterpartyKey = step4.filter(col("counterparty_party_key").isNotNull)
      .withColumn("c1_party_key", lit(null))
      .withColumn("c1_ecif_key", lit(null))

    val step4And5 = step4WithCounterpartyKey.unionByName(step5b)

    // Final processing - combine all party keys
    step4And5
      .withColumn(
        "all_party_keys",
        array(
          col("card_party_key"),
          col("account_party_key"),
          col("cl_party_key"),
          col("counterparty_party_key"),
          col("c1_party_key")
        )
      )
      .withColumn(
        "filtered_party_keys",
        expr("filter(all_party_keys, x -> x is not null)")
      )
      .withColumn(
        "distinct_party_keys",
        expr("array_distinct(filtered_party_keys)")
      )
      .withColumn(
        "acct_holders_all",
        concat_ws(";", col("distinct_party_keys"))
      )
      .select(
       // col("txn_id"),
        col("account_number"),
        col("holding_branch_key"),
        col("card_number"),
        col("opp_account_number"),
        col("opp_branch_key"),
        col("acct_holders_all")
      )
  }

  def getCustomer2AccountHolderCifId(
                                      unifiedTxnDs: Dataset[EsdlUnifiedTxn],
                                      partyProdDs: Dataset[EsdlPartyProd],
                                      accOpenDtDs: Dataset[EsdlAccOpenDate]
                                    ): DataFrame = {

    // Define product type constants
    val depositProductTypes = Seq("DEP", "PDEP", "PLOA", "CL")
    val cardProductTypes = Seq("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC")
    val excludedFallbackTypes = cardProductTypes ++ depositProductTypes :+ "PPC"

    // Step 1: Select needed columns from unified transactions with explicit aliases
    val unifiedTxnCols = unifiedTxnDs.select(
      col("opp_account_number").alias("txn_opp_account_number"),
      col("opp_branch_key").alias("txn_opp_branch_key"),
      col("product_type_code").alias("txn_product_type_code"),
      col("operation_type").alias("txn_operation_type")
    )

    // Step 1: Handle cases where opp_branch_key is not NULL
    val step1 = unifiedTxnCols.alias("txn")
      .join(
        partyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("holding_branch_key").alias("party_branch_key"),
          col("party_key").alias("deposit_party_key"),
          col("product_type_code").alias("party_product_type_code"),
          col("relation_type_cd").alias("party_relation_type_cd")
        ).alias("party_deposit"),
        col("txn.txn_opp_account_number") === col("party_deposit.party_account_number") &&
          col("txn.txn_opp_branch_key").isNotNull &&
          col("txn.txn_opp_branch_key").cast("int") === col("party_deposit.party_branch_key") &&
          col("party_deposit.party_product_type_code").isin(depositProductTypes: _*) &&
          col("party_deposit.party_relation_type_cd") === "1" &&
          col("txn.txn_product_type_code").isin(depositProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("txn.txn_product_type_code"),
        col("txn.txn_operation_type"),
        col("party_deposit.deposit_party_key")
      )

    // Step 2: Handle cases where opp_branch_key is NULL (first case)
    val step2 = step1.alias("txn")
      .join(
        partyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("holding_branch_key").alias("party_branch_key"),
          col("party_key").alias("null_branch_party_key"),
          col("product_type_code").alias("party_product_type_code"),
          col("relation_type_cd").alias("party_relation_type_cd")
        ).alias("party_null_branch"),
        substring(col("txn.txn_opp_account_number"), -7, 7) === col("party_null_branch.party_account_number") &&
          substring(col("txn.txn_opp_account_number"), 1, 5) === col("party_null_branch.party_branch_key") &&
          col("party_null_branch.party_product_type_code").isin(depositProductTypes: _*) &&
          col("party_null_branch.party_relation_type_cd") === "1" &&
          col("txn.txn_opp_branch_key").isNull &&
          col("txn.txn_product_type_code").isin(cardProductTypes ++ Seq("CL"): _*),
        "left_outer"
      )
      .select(
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("txn.txn_product_type_code"),
        col("txn.txn_operation_type"),
        col("txn.deposit_party_key"),
        coalesce(col("txn.deposit_party_key"), col("party_null_branch.null_branch_party_key")).alias("null_branch_party_key")
      )

    // Step 2a: Special handling for PDEP and PLOA
    val step2a = step2.alias("txn")
      .join(
        partyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("holding_branch_key").alias("party_branch_key"),
          col("party_key").alias("special_case_party_key"),
          col("product_type_code").alias("party_product_type_code"),
          col("relation_type_cd").alias("party_relation_type_cd")
        ).alias("party_special"),
        regexp_replace(substring(col("txn.txn_opp_account_number"), -12, 12), "^0+", "") ===
          regexp_replace(col("party_special.party_account_number"), "^0+", "") &&
          substring(col("txn.txn_opp_account_number"), 1, 5) === col("party_special.party_branch_key") &&
          col("party_special.party_product_type_code").isin(depositProductTypes: _*) &&
          col("party_special.party_relation_type_cd") === "1" &&
          col("txn.txn_opp_branch_key").isNull &&
          col("txn.txn_product_type_code").isin("PDEP", "PLOA"),
        "left_outer"
      )
      .select(
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("txn.txn_product_type_code"),
        col("txn.txn_operation_type"),
        col("txn.deposit_party_key"),
        col("txn.null_branch_party_key"),
        coalesce(col("txn.null_branch_party_key"), col("party_special.special_case_party_key")).alias("special_case_party_key")
      )

    // Step 3: Handle card transactions when opp_branch_key is NULL
    val step3 = step2a.alias("txn")
      .join(
        partyProdDs.select(
          col("account_number").alias("party_account_number"),
          col("party_key").alias("card_party_key"),
          col("product_type_code").alias("party_product_type_code"),
          col("relation_type_cd").alias("party_relation_type_cd")
        ).alias("party_card"),
        col("txn.txn_opp_account_number") === col("party_card.party_account_number") &&
          col("party_card.party_product_type_code").isin(cardProductTypes: _*) &&
          col("party_card.party_relation_type_cd") === "1" &&
          col("txn.txn_opp_branch_key").isNull &&
          !col("txn.txn_operation_type").isin("U5", "U6") &&
          col("txn.txn_product_type_code").isin(cardProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.txn_opp_account_number").alias("opp_account_number"),
        col("txn.txn_opp_branch_key").alias("opp_branch_key"),
        col("txn.txn_product_type_code"),
        col("txn.deposit_party_key"),
        col("txn.null_branch_party_key"),
        col("txn.special_case_party_key"),
        coalesce(col("txn.special_case_party_key"), col("party_card.card_party_key")).alias("card_party_key")
      )

    // Step 4: Fallback to account open date lookup - FIXED HERE
    val fallback = step3.alias("step3")
      .filter(col("card_party_key").isNull && !col("txn_product_type_code").isin(excludedFallbackTypes: _*))
      .join(
        accOpenDtDs.select(
          col("curr_plc_acct_num").alias("acc_open_curr_plc_acct_num"),
          col("holding_branch_key_source").alias("acc_open_holding_branch_key_source"),
          col("ecif_composite_key").alias("acc_open_ecif_composite_key"),
          col("product_type_code").alias("acc_open_product_type_code")
        ).alias("acc_open"),
        col("step3.opp_account_number") === col("acc_open.acc_open_curr_plc_acct_num") &&
          col("step3.opp_branch_key").cast("int") === col("acc_open.acc_open_holding_branch_key_source") &&
          col("acc_open.acc_open_product_type_code") === "CL",
        "left_outer"
      )
      .join(
        partyProdDs.select(
          col("ecif_composite_key").alias("party_ecif_composite_key"),
          col("party_key").alias("fallback_party_key"),
          col("product_type_code").alias("party_product_type_code"),
          col("relation_type_cd").alias("party_relation_type_cd")
        ).alias("party_cl"),
        col("acc_open.acc_open_ecif_composite_key") === col("party_cl.party_ecif_composite_key") &&
          col("party_cl.party_product_type_code") === "CL" &&
          col("party_cl.party_relation_type_cd") === "1",
        "left_outer"
      )
      .select(
        col("step3.opp_account_number"),
        col("step3.opp_branch_key"),
        col("step3.deposit_party_key"),
        col("step3.null_branch_party_key"),
        col("step3.special_case_party_key"),
        col("step3.card_party_key"),
        col("party_cl.fallback_party_key")
      )

    // Combine all results with explicit column selection
    val allResults = step3.join(fallback, Seq("opp_account_number", "opp_branch_key"), "left")
      .select(
        col("opp_account_number"),
        col("opp_branch_key"),
        coalesce(col("step3.deposit_party_key"), lit(null)).alias("deposit_party_key"),
        coalesce(col("step3.null_branch_party_key"), lit(null)).alias("null_branch_party_key"),
        coalesce(col("step3.special_case_party_key"), lit(null)).alias("special_case_party_key"),
        coalesce(col("step3.card_party_key"), lit(null)).alias("card_party_key"),
        coalesce(col("party_cl.fallback_party_key"), lit(null)).alias("fallback_party_key")
      )

    // Final aggregation of party keys
    allResults
      .withColumn(
        "all_party_keys",
        array(
          col("deposit_party_key"),
          col("null_branch_party_key"),
          col("special_case_party_key"),
          col("card_party_key"),
          col("fallback_party_key")
        )
      )
      .withColumn(
        "filtered_party_keys",
        expr("filter(all_party_keys, x -> x is not null)")
      )
      .withColumn(
        "distinct_party_keys",
        array_distinct(col("filtered_party_keys"))
      )
      .withColumn(
        "customer2_account_holder_cif_id",
        when(functions.size(col("distinct_party_keys")) > 0,
          concat_ws(";", col("distinct_party_keys")))
          .otherwise(lit(null))
      )
      .select(
        col("opp_account_number"),
        col("opp_branch_key"),
        col("customer2_account_holder_cif_id")
      )
  }

  def getCustomer2AccountStatus(
                                 transactions: Dataset[EsdlTransaction],
                                 accounts: Dataset[EsdlAccount],
                                 accOpenDates: Dataset[EsdlAccOpenDate]
                               ): DataFrame = {

    // Define product type constants
    val depositProductTypes = Seq("PDFP", "DEP", "PLOA", "CL")
    val cardProductTypes = Seq("CARD", "CC", "PCFC", "SVSA", "VISA", "PP")
    val excludedOperationTypes = Seq("U5", "U6")

    // Step 1: Select needed columns and rename for clarity
    val transactionCols = transactions.select(
      col("txn_id").alias("txn_txn_id"),
      col("opp_account_number").alias("txn_opp_account_number"),
      col("opp_branch_key").alias("txn_opp_branch_key"),
      col("operation_type").alias("txn_operation_type")
    )

    val accountCols = accounts.select(
      col("account_number").alias("acc_account_number"),
      col("holding_branch_key").alias("acc_holding_branch_key"),
      col("status_code").alias("acc_status_code"),
      col("product_type_code").alias("acc_product_type"),
      col("ecif_composite_key").alias("acc_ecif_composite_key")
    )

    // Step 1: Handle cases where opp_branch_key is not NULL
    val step1 = transactionCols.alias("txn")
      .join(
        accountCols.alias("acc"),
        col("txn.txn_opp_account_number") === col("acc.acc_account_number") &&
          col("txn.txn_opp_branch_key") === col("acc.acc_holding_branch_key") &&
          col("acc.acc_product_type").isin(depositProductTypes: _*) &&
          col("txn.txn_opp_branch_key").isNotNull,
        "left_outer"
      )
      .select(
        col("txn.txn_txn_id"),
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("txn.txn_operation_type"),
        col("acc.acc_status_code").alias("deposit_status_code")
      )

    // Step 2: Handle card transactions when opp_branch_key is NULL
    val step2 = step1.alias("txn")
      .join(
        accountCols.alias("card_acc"),
        col("txn.txn_opp_account_number") === col("card_acc.acc_account_number") &&
          col("card_acc.acc_product_type").isin(cardProductTypes: _*) &&
          col("txn.txn_opp_branch_key").isNull &&
          !col("txn.txn_operation_type").isin(excludedOperationTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.txn_txn_id"),
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("txn.deposit_status_code"),
        coalesce(col("txn.deposit_status_code"), col("card_acc.acc_status_code")).alias("combined_status_code")
      )

    // Step 3: Fallback to account open date lookup
    val accOpenCols = accOpenDates.select(
      col("curr_plc_acct_num").alias("open_curr_plc_acct_num"),
      col("holding_branch_key_source").alias("open_holding_branch_key_source"),
      col("ecif_composite_key").alias("open_ecif_composite_key"),
      col("product_type_code").alias("open_account_product_type")
    )

    val fallback = step2.alias("txn")
      .filter(col("combined_status_code").isNull)
      .join(
        accOpenCols.alias("acc_open"),
        col("txn.txn_opp_account_number") === col("acc_open.open_curr_plc_acct_num") &&
          col("txn.txn_opp_branch_key").cast("int") === col("acc_open.open_holding_branch_key_source") &&
          col("acc_open.open_account_product_type") === "CL",
        "left_outer"
      )
      .join(
        accountCols.alias("cl_acc"),
        col("acc_open.open_ecif_composite_key") === col("cl_acc.acc_ecif_composite_key") &&
          col("cl_acc.acc_product_type") === "CL",
        "left_outer"
      )
      .select(
        col("txn.txn_txn_id"),
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("txn.deposit_status_code"),
        col("txn.combined_status_code"),
        col("cl_acc.acc_status_code").alias("fallback_status_code")
      )

    // Combine all results
    val allResults = step2.alias("step2")
      .join(
        fallback.alias("fallback"),
        col("step2.txn_txn_id") === col("fallback.txn_txn_id"),
        "left"
      )
      .select(
        col("step2.txn_txn_id").alias("txn_id"),
        col("step2.txn_opp_account_number").alias("opp_account_number"),
        col("step2.txn_opp_branch_key").alias("opp_branch_key"),
        col("step2.deposit_status_code"),
        col("step2.combined_status_code"),
        col("fallback.fallback_status_code")
      )

    // Final status determination
    allResults
      .withColumn(
        "all_status_codes",
        array(
          col("deposit_status_code"),
          col("combined_status_code"),
          col("fallback_status_code")
        )
      )
      .withColumn(
        "filtered_status_codes",
        expr("filter(all_status_codes, x -> x is not null)")
      )
      .withColumn(
        "distinct_status_codes",
        array_distinct(col("filtered_status_codes"))
      )
      .withColumn(
        "customer2_account_status",
        when(functions.size(col("distinct_status_codes")) === 1,
          element_at(col("distinct_status_codes"), 1))
          .otherwise(lit(null))
      )
      .select(
        col("txn_id"),
        col("opp_account_number"),
        col("opp_branch_key"),
        col("customer2_account_status")
      )
  }

  def getCustomer2AccountCurrencyCode(
                                       transactions: Dataset[EsdlTransaction],
                                       accounts: Dataset[EsdlAccount],
                                       accOpenDates: Dataset[EsdlAccOpenDate]
                                     ): DataFrame = {

    // Define product type constants
    val depositProductTypes = Seq("PDP", "DEP", "PLOA", "CL")
    val cardProductTypes = Seq("CARD", "CC", "PCFC", "SVSA", "VISA", "PP")
    val olbSourceSystem = "OLB"

    // Step 1: Handle cases where opp_branch_key is not NULL (deposit accounts)
    val depositAccountCurrency = transactions.alias("txn")
      .select(
        col("txn_id").alias("txn_txn_id"),
        col("opp_account_number").alias("txn_opp_account_number"),
        col("opp_branch_key").alias("txn_opp_branch_key"),
        col("source_system_cd").alias("txn_source_system_cd")
      )
      .filter(col("txn_opp_branch_key").isNotNull)
      .join(
        accounts.select(
          col("account_number").alias("dep_acc_account_number"),
          col("holding_branch_key").alias("dep_acc_holding_branch_key"),
          col("acct_curr_cd").alias("dep_acc_acct_curr_cd"),
          col("product_type_code").alias("dep_acc_product_type_code")
        ).alias("deposit_acc"),
        col("txn_opp_account_number") === col("deposit_acc.dep_acc_account_number") &&
          col("txn_opp_branch_key") === col("deposit_acc.dep_acc_holding_branch_key") &&
          col("deposit_acc.dep_acc_product_type_code").isin(depositProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn_txn_id"),
        col("txn_opp_account_number"),
        col("txn_opp_branch_key"),
        col("txn_source_system_cd"),
        col("deposit_acc.dep_acc_acct_curr_cd").alias("currency_code"),
        lit("deposit_account").alias("source_type")
      )

    // Step 2: Handle cases where opp_branch_key is NULL (card accounts)
    val cardAccountCurrency = transactions.alias("txn")
      .select(
        col("txn_id").alias("txn_txn_id"),
        col("opp_account_number").alias("txn_opp_account_number"),
        col("opp_branch_key").alias("txn_opp_branch_key"),
        col("source_system_cd").alias("txn_source_system_cd")
      )
      .filter(col("txn_opp_branch_key").isNull)
      .join(
        accounts.select(
          col("account_number").alias("card_acc_account_number"),
          col("acct_curr_cd").alias("card_acc_acct_curr_cd"),
          col("product_type_code").alias("card_acc_product_type_code")
        ).alias("card_acc"),
        col("txn_opp_account_number") === col("card_acc.card_acc_account_number") &&
          col("card_acc.card_acc_product_type_code").isin(cardProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn_txn_id"),
        col("txn_opp_account_number"),
        col("txn_opp_branch_key"),
        col("txn_source_system_cd"),
        col("card_acc.card_acc_acct_curr_cd").alias("currency_code"),
        lit("card_account").alias("source_type")
      )

    // Combine initial results
    val initialResults = depositAccountCurrency.unionByName(cardAccountCurrency)

    // Step 3: Identify unmatched records
    val unmatchedTxnKeys = transactions
      .select(
        col("opp_account_number").alias("unmatched_opp_account_number"),
        col("opp_branch_key").alias("unmatched_opp_branch_key")
      )
      .except(initialResults.select(
        col("txn_opp_account_number").alias("unmatched_opp_account_number"),
        col("txn_opp_branch_key").alias("unmatched_opp_branch_key")
      ))

    // Step 4: Fallback lookup for OLB transactions
    val fallbackCurrency = transactions.alias("txn")
      .select(
        col("txn_id").alias("txn_txn_id"),
        col("opp_account_number").alias("txn_opp_account_number"),
        col("opp_branch_key").alias("txn_opp_branch_key"),
        col("source_system_cd").alias("txn_source_system_cd")
      )
      .join(unmatchedTxnKeys.alias("unmatched"),
        col("txn_opp_account_number") === col("unmatched.unmatched_opp_account_number") &&
          col("txn_opp_branch_key") === col("unmatched.unmatched_opp_branch_key"),
        "inner"
      )
      .filter(col("txn_source_system_cd") === olbSourceSystem)
      .join(
        accOpenDates.select(
          col("curr_plc_acct_num").alias("open_curr_plc_acct_num"),
          col("holding_branch_key_source").alias("open_holding_branch_key_source"),
          col("ecif_composite_key").alias("open_ecif_composite_key"),
          col("product_type_code").alias("open_product_type_code")
        ).alias("acc_open"),
        col("txn_opp_account_number") === col("acc_open.open_curr_plc_acct_num") &&
          col("txn_opp_branch_key").cast("int") === col("acc_open.open_holding_branch_key_source") &&
          col("acc_open.open_product_type_code") === "CL",
        "left_outer"
      )
      .join(
        accounts.select(
          col("ecif_composite_key").alias("cl_acc_ecif_composite_key"),
          col("acct_curr_cd").alias("cl_acc_acct_curr_cd"),
          col("product_type_code").alias("cl_acc_product_type_code")
        ).alias("cl_acc"),
        col("acc_open.open_ecif_composite_key") === col("cl_acc.cl_acc_ecif_composite_key") &&
          col("cl_acc.cl_acc_product_type_code") === "CL",
        "left_outer"
      )
      .select(
        col("txn_txn_id"),
        col("txn_opp_account_number"),
        col("txn_opp_branch_key"),
        col("txn_source_system_cd"),
        col("cl_acc.cl_acc_acct_curr_cd").alias("currency_code"),
        lit("fallback_lookup").alias("source_type")
      )

    // Combine all currency lookup results
    val allCurrencyResults = initialResults.unionByName(fallbackCurrency)

    // Determine final currency code for each account
    val finalCurrencyCodes = allCurrencyResults
      .groupBy("txn_opp_account_number", "txn_opp_branch_key")
      .agg(
        collect_set("currency_code").alias("currency_codes"),
        collect_set("source_type").alias("source_types")
      )
      .withColumn("currency_code_count", functions.size(col("currency_codes")))
      .withColumn(
        "customer2_account_currency_code",
        when(col("currency_code_count") === 1, element_at(col("currency_codes"), 1))
          .otherwise(lit(null))
      )
      .select(
        col("txn_opp_account_number").alias("opp_account_number"),
        col("txn_opp_branch_key").alias("opp_branch_key"),
        col("customer2_account_currency_code")
      )

    // Join with original transactions to preserve all fields
    transactions.alias("orig_txn")
      .join(
        finalCurrencyCodes.alias("currency"),
        col("orig_txn.opp_account_number") === col("currency.opp_account_number") &&
          col("orig_txn.opp_branch_key") === col("currency.opp_branch_key"),
        "left_outer"
      )
      .select(
       // col("orig_txn.*"),
        col("currency.customer2_account_currency_code"),
        col("orig_txn.account_number"),
        col("orig_txn.holding_branch_key")
      )
     // .drop(col("orig_txn.source_system_cd"))
     // .drop(col("orig_txn.channel_cd"))
  }

  def calculateOppProdType(
                            partyProdDs: Dataset[EsdlPartyProd],
                            transactions: Dataset[EsdlTransaction],
                            accOpenDates: Dataset[EsdlAccOpenDate]
                          ): DataFrame = {

    // Helper function to clean account numbers
    def cleanAccountNumber(col: Column): Column = regexp_replace(col, "^0+", "")

    // Define product type constants
    val depositCardProductTypes = Seq("DEP", "PDFP", "CARD", "CC", "PCFC", "SVSA", "VISA", "PLOA")
    val cardProductTypes = Seq("DEP", "CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "CL")
    val specialProductTypes = Seq("PDFP", "PLOA")

    // Step 1: Select only needed columns
    val transactionCols = transactions.select(
      col("txn_id").alias("txn_txn_id"),
      col("opp_account_number").alias("txn_opp_account_number"),
      col("opp_branch_key").alias("txn_opp_branch_key")
    )

    val partyProdCols = partyProdDs.select(
      col("account_number").alias("party_account_number"),
      col("holding_branch_key").alias("party_holding_branch_key"),
      col("product_type_code").alias("party_product_type_code")
    )

    // Step 2: Handle cases where opp_branch_key is not null
    val branchKeyNotNullJoin = transactionCols.alias("txn")
      .filter(col("txn.txn_opp_branch_key").isNotNull)
      .join(
        partyProdCols.alias("party"),
        col("txn.txn_opp_account_number") === col("party.party_account_number") &&
          col("txn.txn_opp_branch_key").cast("int") === col("party.party_holding_branch_key") &&
          col("party.party_product_type_code").isin(depositCardProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.txn_txn_id"),
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("party.party_product_type_code").alias("party_prod_type")
      )

    // Step 3: Handle cases where opp_branch_key is null (first case)
    val branchKeyNullJoin1 = transactionCols.alias("txn")
      .filter(col("txn.txn_opp_branch_key").isNull)
      .join(
        partyProdCols.alias("party"),
        col("txn.txn_opp_account_number") === cleanAccountNumber(col("party.party_account_number")) &&
          col("party.party_product_type_code").isin(depositCardProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.txn_txn_id"),
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("party.party_product_type_code").alias("party_prod_type")
      )

    // Step 4: Handle cases where opp_branch_key is null (second case)
    val branchKeyNullJoin2 = transactionCols.alias("txn")
      .filter(col("txn.txn_opp_branch_key").isNull)
      .join(
        partyProdCols.alias("party"),
        substring(col("txn.txn_opp_account_number"), -7, 7) === col("party.party_account_number") &&
          substring(col("txn.txn_opp_account_number"), 1, 5) === col("party.party_holding_branch_key") &&
          col("party.party_product_type_code").isin(cardProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.txn_txn_id"),
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("party.party_product_type_code").alias("party_prod_type")
      )

    // Step 5: Handle special cases for PDEP and PLOA
    val specialCasesJoin = transactionCols.alias("txn")
      .filter(col("txn.txn_opp_branch_key").isNull)
      .join(
        partyProdCols.alias("party"),
        substring(col("txn.txn_opp_account_number"), -12, 12) === col("party.party_account_number") &&
          substring(col("txn.txn_opp_account_number"), 1, 5) === col("party.party_holding_branch_key") &&
          col("party.party_product_type_code").isin(specialProductTypes: _*),
        "left_outer"
      )
      .select(
        col("txn.txn_txn_id"),
        col("txn.txn_opp_account_number"),
        col("txn.txn_opp_branch_key"),
        col("party.party_product_type_code").alias("party_prod_type")
      )

    // Combine all results
    val allPartyProdResults = branchKeyNotNullJoin
      .unionByName(branchKeyNullJoin1)
      .unionByName(branchKeyNullJoin2)
      .unionByName(specialCasesJoin)

    // Determine distinct product types
    val distinctProdTypes = allPartyProdResults
      .groupBy("txn_opp_account_number", "txn_opp_branch_key")
      .agg(
        collect_set("party_prod_type").alias("prod_type_codes")
      )
      .withColumn(
        "party_prod_type_result",
        when(functions.size(col("prod_type_codes")) === 1, element_at(col("prod_type_codes"), 1))
          .otherwise(lit(null))
      )

    // Step 6: Handle loan account case
    val loanAccountCheck = transactions.alias("txn")
      .select(
        col("txn_id").alias("txn_txn_id"),
        col("opp_account_number").alias("txn_opp_account_number"),
        col("opp_branch_key").alias("txn_opp_branch_key")
      )
      .join(
        accOpenDates.select(
          col("curr_plc_acct_num").alias("open_curr_plc_acct_num"),
          col("holding_branch_key_source").alias("open_holding_branch_key_source"),
          col("product_type_code").alias("open_product_type_code")
        ).alias("acc_open"),
        col("txn_opp_account_number") === col("acc_open.open_curr_plc_acct_num") &&
          col("txn_opp_branch_key").cast("int") === col("acc_open.open_holding_branch_key_source") &&
          col("acc_open.open_product_type_code") === "CL",
        "left_outer"
      )
      .select(
        col("txn_txn_id"),
        col("txn_opp_account_number"),
        col("txn_opp_branch_key"),
        col("acc_open.open_product_type_code").alias("loan_prod_type")
      )

    // Combine with party product results
    val combinedResults = loanAccountCheck.alias("txn")
      .join(
        distinctProdTypes.alias("prod_types"),
        col("txn.txn_opp_account_number") === col("prod_types.txn_opp_account_number") &&
          col("txn.txn_opp_branch_key") === col("prod_types.txn_opp_branch_key"),
        "left_outer"
      )

    // Final determination of opposing product type
    combinedResults
      .withColumn(
        "opp_prod_type",
        when(col("party_prod_type_result").isNotNull, col("party_prod_type_result"))
          .when(col("loan_prod_type").isNotNull, col("loan_prod_type"))
          .otherwise(lit(null))
      )
      .select(
        col("txn.txn_txn_id").alias("txn_id"),
        col("txn.txn_opp_account_number").alias("opp_account_number"),
        col("txn.txn_opp_branch_key").alias("opp_branch_key"),
        col("opp_prod_type")
      )
  }



  // Helper function to clean account numbers by dropping leading zeros
  def cleanAccountNumber(accountNumber: Column): Column = {
    regexp_replace(accountNumber, "^0+", "")
  }


    // Join all datasets on common keys (transaction_id, account_number, etc.)
    // Adjust join conditions based on your actual schema

    def joinAllResults(transformations: Map[String, DataFrame]): DataFrame = {
      // Get all transformation results with proper column selection
      val esdlTxnMapping = transformations("esdlTxnMapping")
      val unifiedAttMap = transformations("unifiedAttMap")
      val ecifKeyAttMap = transformations("ecifKeyAttMap")
      val cust1AccHolderCifId = transformations("cust1AccHolderCifId")
      val accHoldersAll = transformations("accHoldersAll")
      val customer2AccountHolderCifId = transformations("customer2AccountHolderCifId")
      val customer2AccountStatus = transformations("customer2AccountStatus")
      val customer2AccountCurrencyCode = transformations("customer2AccountCurrencyCode")
      val oppProdTypeResult = transformations("oppProdTypeResult")

      // Perform the joins with explicit column references
      val joinedResults = esdlTxnMapping
        // Join with unified attributes
        .join(unifiedAttMap,
          esdlTxnMapping("opp_account_number") === unifiedAttMap("party_account_number"),
          "left_outer")
        // Join with ECIF attributes
        .join(ecifKeyAttMap,
          esdlTxnMapping("opp_account_number") === ecifKeyAttMap("account_number_utxn"),
          "left_outer")
        // Join with customer1 account holder
        .join(cust1AccHolderCifId,
          esdlTxnMapping("opp_account_number") === cust1AccHolderCifId("account_number"),
          "left_outer")
        // Join with all account holders
        .join(accHoldersAll,
          esdlTxnMapping("account_number2") === accHoldersAll("account_number"),
          "left_outer")
        // Join with customer2 account holder CIF ID
        .join(customer2AccountHolderCifId,
          esdlTxnMapping("opp_account_number") === customer2AccountHolderCifId("opp_account_number") &&
            esdlTxnMapping("opp_branch_key") === customer2AccountHolderCifId("opp_branch_key"),
          "left_outer")
        // Join with customer2 account status
        .join(customer2AccountStatus,
          esdlTxnMapping("opp_account_number") === customer2AccountStatus("opp_account_number") &&
            esdlTxnMapping("opp_branch_key") === customer2AccountStatus("opp_branch_key"),
          "left_outer")
        // Join with customer2 account currency
        .join(customer2AccountCurrencyCode,
          esdlTxnMapping("account_number2") === customer2AccountCurrencyCode("account_number") &&
            esdlTxnMapping("holding_branch_key2") === customer2AccountCurrencyCode("holding_branch_key"),
          "left_outer")
        // Join with opposing product type
        .join(oppProdTypeResult,
          esdlTxnMapping("opp_account_number") === oppProdTypeResult("opp_account_number") &&
            esdlTxnMapping("opp_branch_key") === oppProdTypeResult("opp_branch_key"),
          "left_outer")

      joinedResults
    }
}
