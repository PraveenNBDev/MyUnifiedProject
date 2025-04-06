import Models.{CddprfRskScoreLatestFull, EsdlAccOpenDate, EsdlAccount, EsdlPartyProd, EsdlPartyXRef, EsdlTransaction, EsdlUnifiedTxn}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



object EsdlUnifiedTxnGenerator {

  def generateUnifiedTxn(
                          spark: SparkSession,
                          esdlTransactionDF: Dataset[EsdlTransaction],
                          esdlPartyProdDF: Dataset[EsdlPartyProd],
                          esdlAccountOpenDateDF: Dataset[EsdlAccOpenDate],
                          riskScoringDF: Dataset[CddprfRskScoreLatestFull],
                          esdlPartyXRef: Dataset[EsdlPartyXRef]
                        ): Dataset[EsdlUnifiedTxn] = {

    import spark.implicits._

    // Step 1: Prepare base fields from esdl_transaction
    val baseTxnDF = esdlTransactionDF.select(
      col("account_number"),
      col("holding_branch_key"),
      col("product_type_code"),
      col("opp_account_number"),
      col("opp_branch_key"),
      col("operation_type"),
      col("card_number"),
      col("execution_local_date_time"),
      col("cr_dr_code")
    ).distinct()

    // Step 2: Generate ECIF Key with business logic
    val withEcifKeyDF = generateEcifKey(baseTxnDF, esdlPartyProdDF, esdlAccountOpenDateDF, riskScoringDF)
    // Step 3: Generate Customer1 Account Holder CIF ID
    val withCustomer1DF = generateCustomer1CifId(withEcifKeyDF, esdlPartyProdDF, esdlAccountOpenDateDF)
    // Step 4: Generate Customer2 Account Holder CIF ID
    val withCustomer2DF = generateCustomer2CifId(withCustomer1DF, esdlPartyProdDF, esdlAccountOpenDateDF)
    val withCustomer2DF1 = generateConductorKey(withCustomer2DF, esdlPartyProdDF, esdlPartyXRef)
    // Step 5: Convert to final case class with proper mapping
    val finalUnifiedCols = withCustomer2DF1
      .withColumn("creditor_id",
        when(col("cr_dr_code").isin("D"), col("customer2_Account_holder_cif_id"))
          .when(col("cr_dr_code").isin("C"), col("customer1_Account_holder_cif_id"))
          .otherwise(lit("null")))

    finalUnifiedCols.select(
      col("account_number"),
      col("holding_branch_key"),
      col("product_type_code"),
      col("opp_account_number"),
      col("opp_branch_key"),
      col("operation_type"),
      col("primary_party_key"),
      col("third_party_cif_id"),
      col("on_behalf_of_ind"),
      col("ecif_key"),
      col("card_number"),
      col("execution_local_date_time"),
      col("cr_dr_code"),
      col("creditor_id"),
      col("customer1_Account_holder_cif_id"),
      col("customer2_Account_holder_cif_id")
    ).as[EsdlUnifiedTxn]
  }

  def generateEcifKey(unifiedTxnDs: DataFrame,
                      esdlPartyProdDs: Dataset[EsdlPartyProd],
                      esdlAccOpenDt: Dataset[EsdlAccOpenDate],
                      riskScoring: Dataset[CddprfRskScoreLatestFull]): DataFrame = {

    // Step 1: Normalize account numbers
    val transactions = unifiedTxnDs
      .withColumn("account_number", regexp_replace(col("account_number"), "^0+", ""))
      .withColumn("opp_account_number", regexp_replace(col("opp_account_number"), "^0+", ""))

    // Step 2: Alias esdlPartyProdDs and rename to avoid ambiguity
    val esdlPartyProdAlias1 = esdlPartyProdDs.as("epp1")
    val transactionsAlias = transactions.as("txn")

    val partyKeyDF = transactionsAlias.join(esdlPartyProdAlias1,
        (col("txn.account_number") === col("epp1.account_number") || col("txn.opp_account_number") === col("epp1.account_number")) &&
          col("txn.product_type_code") === col("epp1.product_type_code") &&
          col("txn.holding_branch_key") === col("epp1.holding_branch_key") &&
          col("epp1.relation_type_cd") === lit("1"), "left")
      .selectExpr("txn.*", "epp1.party_key as party_key_1")

    // Step 3: Join with esdlAccOpenDt and get ecif_composite_key if needed
    val esdlAccOpenDtAlias = esdlAccOpenDt.as("eao")
    val partyKeyAlias = partyKeyDF.as("pk")

    val ecifKeyDF = partyKeyAlias.join(esdlAccOpenDtAlias,
        (col("pk.account_number") === col("eao.curr_plc_acct_num") || col("pk.opp_account_number") === col("eao.curr_plc_acct_num")) &&
          col("pk.product_type_code") === lit("CL"), "left")
      .selectExpr("pk.*", "eao.ecif_composite_key as acc_open_ecif_composite_key")
      .withColumn("merged_ecif_composite_key", when(col("party_key_1").isNotNull, col("party_key_1"))
        .otherwise(col("acc_open_ecif_composite_key")))

    // Step 4: Join again with esdlPartyProd to get final_party_key
    val esdlPartyProdAlias2 = esdlPartyProdDs.as("epp2")
    val ecifKeyAlias = ecifKeyDF.as("ek")

    val finalPartyKeyDF = ecifKeyAlias.join(esdlPartyProdAlias2,
        col("ek.merged_ecif_composite_key") === col("epp2.ecif_composite_key") &&
          col("epp2.product_type_code") === lit("CL") &&
          col("epp2.relation_type_cd") === lit("1"), "left")
      .selectExpr("ek.*", "epp2.party_key as epp2_party_key", "epp2.ecif_composite_key as epp2_ecif_composite_key")
      .withColumn("final_party_key", when(col("epp2_party_key").isNotNull, col("epp2_party_key"))
        .otherwise(col("epp2_ecif_composite_key")))

    // Step 5: Join with risk scoring and get latest risk score
    val finalPartyAlias = finalPartyKeyDF.as("fp")
    val riskScoringAlias = riskScoring.as("rs")

    val riskDF = finalPartyAlias.join(riskScoringAlias, col("fp.final_party_key") === col("rs.party_key"), "left")
      .withColumn("latest_risk_score", max(col("rs.fctp_process_date")).over(Window.partitionBy("fp.final_party_key")))
      .orderBy(desc("latest_risk_score"))

    // Step 6: Rank and get the lowest final_party_key
    val finalDF = riskDF.withColumn("rank", row_number().over(Window.partitionBy("latest_risk_score").orderBy(col("final_party_key"))))
      .filter(col("rank") === 1)
      .withColumn("final_ecif_key", when(col("final_party_key").isNotNull, col("final_party_key"))
        .otherwise(col("fp.merged_ecif_composite_key")))

    // Step 7: Filter based on product_type_code
    val finalFilteredDF = finalDF.withColumn("final_ecif_key", when(
      col("final_ecif_key").isNull && !col("fp.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PPP", "PPC", "DEP", "PDEP", "CL", "PLOA"),
      lit(null)).otherwise(col("final_ecif_key")))

    finalFilteredDF

  }

  private def generateCustomer1CifId(
                                      txnDF: DataFrame,
                                      partyProdDF: Dataset[EsdlPartyProd],
                                      accountOpenDF: Dataset[EsdlAccOpenDate]
                                    ): DataFrame = {

    val txnAlias = txnDF
      .withColumn("account_number", regexp_replace(col("account_number"), "^0+", ""))
      .withColumn("holding_branch_key", col("holding_branch_key").cast("int"))
      .withColumn("opp_branch_key", col("opp_branch_key"))
      .alias("txn")

    val partyAlias = partyProdDF
      .withColumnRenamed("account_number", "party_account_number")
      .withColumnRenamed("holding_branch_key", "party_holding_branch_key")
      .withColumnRenamed("product_type_code", "party_product_type_code")
      .withColumnRenamed("relation_type_cd", "party_relation_type_cd")
      .withColumnRenamed("ecif_composite_key", "party_ecif_composite_key")
      .withColumnRenamed("party_key", "renamed_party_key")
      .alias("pp")

    val accountOpenAlias = accountOpenDF
      .withColumnRenamed("curr_plc_acct_num", "account_open_acct_num")
      .withColumnRenamed("holding_branch_key_source", "account_open_holding_branch_key")
      .withColumnRenamed("ecif_composite_key", "account_open_ecif_composite_key")
      .alias("ao")

    val partyKeyDF = txnAlias.join(partyAlias,
        col("txn.account_number") === col("pp.party_account_number") &&
          col("txn.holding_branch_key") === col("pp.party_holding_branch_key") &&
          col("txn.product_type_code") === col("pp.party_product_type_code") &&
          col("pp.party_relation_type_cd") === lit("1"), "left")
      .selectExpr("txn.*", "pp.renamed_party_key as joined_party_key", "pp.party_product_type_code as joined_product_type_code")
      .alias("pk")
      .drop(col("txn.product_type_code"))

    val ecifKeyDF = partyKeyDF.join(accountOpenAlias,
        col("pk.account_number") === col("ao.account_open_acct_num") &&
          col("pk.holding_branch_key") === col("ao.account_open_holding_branch_key") &&
          !col("pk.joined_product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL"), "left")
      .withColumn("ecif_key", when(col("pk.joined_party_key").isNotNull, col("pk.joined_party_key"))
        .otherwise(col("ao.account_open_ecif_composite_key")))
      .alias("ek")

    val joinDF = ecifKeyDF.join(
      partyAlias,
      col("ek.ecif_key") === col("pp.party_ecif_composite_key") &&
        col("pp.party_product_type_code") === lit("CL") &&
        col("pp.party_relation_type_cd") === lit("1"),
      "left"
    )

    // Define window over the same fields you would normally group by
    val windowSpec = Window.partitionBy(
      col("ek.account_number"),
      col("ek.holding_branch_key"),
      col("ek.joined_product_type_code"),
      col("ek.opp_branch_key")
    )

    val finalPartyKeyDF = joinDF
      .withColumn("party_keys", collect_list(col("pp.renamed_party_key")).over(windowSpec))
      .withColumn("Customer1_account_holder_cif_id", concat_ws(";", col("party_keys")))
      .withColumn(
        "Customer1_account_holder_cif_id",
        when(col("Customer1_account_holder_cif_id") === "", lit("NULL"))
          .otherwise(col("Customer1_account_holder_cif_id"))
      )
      .drop("party_keys")

    finalPartyKeyDF
  }

  private def generateCustomer2CifId(
                                      txnDF: DataFrame,
                                      partyProdDF: Dataset[EsdlPartyProd],
                                      accountOpenDF: Dataset[EsdlAccOpenDate]
                                    ): DataFrame = {


    // Step 1: When opp_branch_key is not NULL
    val step1 = txnDF
      .filter(col("opp_branch_key").isNotNull && col("product_type_code").isin("DEP", "PDFP", "PLOA", "CL"))
      .join(
        partyProdDF.select(
          col("account_number").as("pp_account_number"),
          col("holding_branch_key").as("pp_holding_branch_key"),
          col("product_type_code").as("pp_product_type_code"),
          col("party_key").as("pp_party_key")
        ),
        col("opp_account_number") === col("pp_account_number") &&
          col("opp_branch_key").cast("int") === col("pp_holding_branch_key") &&
          col("product_type_code") === col("pp_product_type_code"),
        "left"
      )
      .withColumn("step1_party_key", col("pp_party_key"))
      .drop("pp_account_number", "pp_holding_branch_key", "pp_product_type_code", "pp_party_key")
      .withColumn("step2_party_key", lit(null).cast("string"))
      .withColumn("step2a_party_key", lit(null).cast("string"))
      .withColumn("step3_party_key", lit(null).cast("string"))

    // Step 2: When opp_branch_key is NULL for certain product types
    val step2 = txnDF
      .filter(col("opp_branch_key").isNull && col("product_type_code").isin("DEP", "CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "CL"))
      .join(
        partyProdDF.select(
          col("account_number").as("pp_account_number"),
          col("holding_branch_key").as("pp_holding_branch_key"),
          col("product_type_code").as("pp_product_type_code"),
          col("party_key").as("pp_party_key")
        ),
        expr("right(opp_account_number, 7) = pp_account_number") &&
          expr("left(opp_account_number, 5) = pp_holding_branch_key") &&
          col("product_type_code") === col("pp_product_type_code"),
        "left"
      )
      .withColumn("step2_party_key", col("pp_party_key"))
      .withColumn("step1_party_key", lit(null).cast("string"))
      .withColumn("step2a_party_key", lit(null).cast("string"))
      .withColumn("step3_party_key", lit(null).cast("string"))
      .drop("pp_account_number", "pp_holding_branch_key", "pp_product_type_code", "pp_party_key")

    // Step 2a: When opp_branch_key is NULL for PDFP and PLOA products
    val step2a = txnDF
      .filter(col("opp_branch_key").isNull && col("product_type_code").isin("PDFP", "PLOA"))
      .join(
        partyProdDF.select(
          col("account_number").as("pp_account_number"),
          col("holding_branch_key").as("pp_holding_branch_key"),
          col("product_type_code").as("pp_product_type_code"),
          col("party_key").as("pp_party_key")
        ),
        expr("substring(opp_account_number, length(opp_account_number) - 11, 12) = pp_account_number") &&
          expr("left(opp_account_number, 5) = pp_holding_branch_key") &&
          col("product_type_code") === col("pp_product_type_code"),
        "left"
      )
      .withColumn("step2a_party_key", col("pp_party_key"))
      .withColumn("step1_party_key", lit(null).cast("string"))
      .withColumn("step2_party_key", lit(null).cast("string"))
      .withColumn("step3_party_key", lit(null).cast("string"))
      .drop("pp_account_number", "pp_holding_branch_key", "pp_product_type_code", "pp_party_key")

    // Step 3: When opp_branch_key is NULL and operation_type not in ('U5','U6')
    val step3 = txnDF
      .filter(col("opp_branch_key").isNull && !col("operation_type").isin("U5", "U6") &&
        col("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP"))
      .join(
        partyProdDF.select(
          col("account_number").as("pp_account_number"),
          col("product_type_code").as("pp_product_type_code"),
          col("party_key").as("pp_party_key")
        ),
        col("opp_account_number") === col("pp_account_number") &&
          col("product_type_code") === col("pp_product_type_code"),
        "left"
      )
      .withColumn("step3_party_key", col("pp_party_key"))
      .withColumn("step1_party_key", lit(null).cast("string"))
      .withColumn("step2_party_key", lit(null).cast("string"))
      .withColumn("step2a_party_key", lit(null).cast("string"))
      .drop("pp_account_number", "pp_product_type_code", "pp_party_key")

    // Combine all steps and add combined_party_key column
    val combinedSteps = step1
      .union(step2)
      .union(step2a)
      .union(step3)
      .withColumn("combined_party_key",
        coalesce(
          col("step1_party_key"),
          col("step2_party_key"),
          col("step2a_party_key"),
          col("step3_party_key")
        )
      )

    // Step 4: Fallback lookup when party_key not found in previous steps
    val step4 = combinedSteps
      .filter(col("combined_party_key").isNull)
      .filter(!col("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL"))
      .join(
        accountOpenDF.select(
          col("curr_plc_acct_num").as("aod_account_number"),
          col("holding_branch_key_source").as("aod_branch_key"),
          col("product_type_code").as("aod_product_type_code"),
          col("ecif_composite_key").as("aod_ecif_key")
        ),
        col("opp_account_number") === col("aod_account_number") &&
          col("opp_branch_key").cast("int") === col("aod_branch_key") &&
          col("aod_product_type_code") === "CL",
        "left"
      )
      .join(
        partyProdDF.select(
          col("ecif_composite_key").as("pp_ecif_key"),
          col("product_type_code").as("pp_product_type_code"),
          col("relation_type_cd").as("pp_relation_type"),
          col("party_key").as("pp_party_key")
        ),
        col("aod_ecif_key") === col("pp_ecif_key") &&
          col("pp_product_type_code") === "CL" &&
          col("pp_relation_type") === "1",
        "left"
      )
      .withColumn("step4_party_key", col("pp_party_key"))
      .drop("aod_account_number", "aod_branch_key", "aod_product_type_code", "aod_ecif_key",
        "pp_ecif_key", "pp_product_type_code", "pp_relation_type", "pp_party_key")

    // Combine all results with non-null party keys
    val allResults = combinedSteps
      .filter(col("combined_party_key").isNotNull)
      .withColumn("party_key", col("combined_party_key"))
      .withColumn("null_col1", lit("null"))
      .union(
        step4.withColumn("party_key", col("step4_party_key"))
      )

    // Window specification for aggregation
    val windowSpec = Window.partitionBy(
      "account_number",
      "holding_branch_key",
      "product_type_code",
      "opp_account_number",
      "opp_branch_key",
      "operation_type",
      "card_number",
      "execution_local_date_time",
      "cr_dr_code",
      "acc_open_ecif_composite_key",
      "Customer1_account_holder_cif_id",
      "ecif_key"
    )

    // Final result with concatenated party keys
    val result = allResults
      .withColumn(
        "customer2_account_holder_cif_id",
        when(
          col("party_key").isNotNull,
          concat_ws(".", collect_set(col("party_key")).over(windowSpec))
        ).otherwise(lit("NULL"))
      )
      .select(
        col("account_number"),
        col("holding_branch_key"),
        col("product_type_code"),
        col("opp_account_number"),
        col("opp_branch_key"),
        col("operation_type"),
        col("card_number"),
        col("execution_local_date_time"),
        col("cr_dr_code"),
        col("acc_open_ecif_composite_key"),
        col("Customer1_account_holder_cif_id"),
        col("ecif_key"),
        col("customer2_account_holder_cif_id")
      )
      .distinct()

    result
  }


  def generateConductorKey(
                            unifiedTxnDF: DataFrame,
                            partyProdDF: Dataset[EsdlPartyProd],
                            partyXrefDF: Dataset[EsdlPartyXRef]
                          ): DataFrame = {

    val spark = unifiedTxnDF.sparkSession
    import spark.implicits._

    // Step 0: Preprocess base DataFrames
    val txn = unifiedTxnDF
      .withColumn("card_number_trim", regexp_replace(col("card_number"), "^0+", ""))
      .withColumn("account_number_trim", regexp_replace(col("account_number"), "^0+", ""))
      .withColumn("holding_branch_key_trim", regexp_replace(col("holding_branch_key"), "^0+", ""))
      .alias("txn")

    val partyProd = partyProdDF
      .withColumn("account_number_trim", regexp_replace(col("account_number"), "^0+", ""))
      .withColumn("holding_branch_key_trim", regexp_replace(col("holding_branch_key"), "^0+", ""))
      .alias("pp")

    val partyXref = partyXrefDF
      .filter(col("bus_app_id") === "344")
      .withColumn("cross_prefix", substring(col("cross_ref_num"), 0, 4))
      .withColumn("cross_suffix", substring(col("cross_ref_num"), 6, 7))
      .withColumn("match_account_number", concat(col("cross_prefix"), lit("4"), col("cross_suffix")))
      .alias("xref")

    // Step 1: Join using card_number
    import org.apache.spark.sql.expressions.Window

    val step1 = txn
      .join(partyProd,
        col("txn.card_number_trim") === col("pp.account_number_trim") &&
          col("pp.relation_type_cd").isin(1, 3, 5, 9, 11, 31, 36, 37, 38, 40) &&
          col("pp.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PPP", "PPC"),
        "left"
      )
      .withColumn("is_owner", when(col("pp.relation_type_cd") === 1, 1)
        .when(col("pp.relation_type_cd") === 31, 2)
        .otherwise(3))
      .withColumn("row_rank", row_number().over(
        Window.partitionBy(
          col("txn.card_number_trim"),
          col("txn.account_number"),
          col("txn.holding_branch_key"),
          col("txn.opp_account_number"),
          col("txn.opp_branch_key")
        ).orderBy(col("is_owner"))
      ))
      .filter(col("row_rank") === 1)
      .withColumn("step2_key", lit(null).cast("string"))
      .withColumn("step3_key", lit(null).cast("string"))
      .withColumn("step4_key", lit(null).cast("string"))
      .select(
        col("txn.account_number"),
        col("txn.holding_branch_key"),
        col("pp.product_type_code"),
        col("pp.relation_type_cd"),
        col("txn.opp_account_number"),
        col("txn.opp_branch_key"),
        col("txn.operation_type"),
        col("txn.ecif_key"),
        col("txn.card_number_trim").alias("card_number"),
        col("txn.execution_local_date_time"),
        col("txn.cr_dr_code"),
        col("txn.customer1_Account_holder_cif_id"),
        col("txn.customer2_Account_holder_cif_id"),
        col("pp.party_key").as("step1_key"),
        col("step2_key"),
        col("step3_key"),
        col("step4_key")
      )

    val step2 = txn
      .join(partyXref, col("txn.account_number_trim") === col("xref.match_account_number"), "left")
      .withColumn("step1_key", lit(null).cast("string"))
      .withColumn("step3_key", lit(null).cast("string"))
      .withColumn("step4_key", lit(null).cast("string"))
      .select(
        col("account_number"),
        col("holding_branch_key"),
        col("product_type_code"),
        lit("null").as("null_col1"),
        col("opp_account_number"),
        col("opp_branch_key"),
        col("operation_type"),
        col("ecif_key"),
        col("card_number_trim").alias("card_number"),
        col("execution_local_date_time"),
        col("cr_dr_code"),
        col("customer1_Account_holder_cif_id"),
        col("customer2_Account_holder_cif_id"),
        col("xref.party_key").as("step2_key"),
        col("step1_key"),
        col("step3_key"),
        col("step4_key")
      )

    // Step 3: Fallback matching with account_number and holding_branch_key
    val step3 = txn
      .join(partyProd,
        col("txn.account_number_trim") === col("pp.account_number_trim") &&
          (
            (col("txn.holding_branch_key_trim").isNotNull && col("txn.holding_branch_key_trim") === col("pp.holding_branch_key_trim")) ||
              col("txn.holding_branch_key_trim").isNull
            ) &&
          col("pp.relation_type_cd").isin(1, 3, 9) &&
          col("pp.product_type_code").isin("DEP", "PDEP", "CL", "PLOA"),
        "left"
      ).withColumn("step1_key", lit(null).cast("string"))
      .withColumn("step2_key", lit(null).cast("string"))
      .withColumn("step4_key", lit(null).cast("string"))
      .select(
        col("txn.account_number"),
        col("txn.holding_branch_key"),
        col("txn.product_type_code"),
        col("pp.relation_type_cd"),
        col("opp_account_number"),
        col("opp_branch_key"),
        col("operation_type"),
        col("ecif_key"),
        col("txn.card_number_trim").alias("card_number"),
        col("txn.execution_local_date_time"),
        col("cr_dr_code"),
        col("customer1_Account_holder_cif_id"),
        col("customer2_Account_holder_cif_id"),
        col("pp.party_key").as("step3_key"),
        col("step1_key"),
        col("step2_key"),
        col("step4_key")
      )

    val step4 = txn
      .join(partyProd,
        col("txn.card_number_trim") === col("pp.account_number_trim") &&
          col("pp.relation_type_cd").isin(1, 3, 5, 9, 11, 31, 36, 37, 38, 40) &&
          col("pp.product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PPP", "PPC"),
        "left"
      )
      .withColumn("is_owner", when(col("pp.relation_type_cd") === 1, 1)
        .when(col("pp.relation_type_cd") === 31, 2)
        .otherwise(3))
      .withColumn("row_rank", row_number().over(
        Window.partitionBy(
          col("txn.card_number_trim"),
          col("txn.account_number"),
          col("txn.holding_branch_key"),
          col("txn.opp_account_number"),
          col("txn.opp_branch_key")
        ).orderBy(col("is_owner"))
      ))
      .filter(col("row_rank") === 1)
      .withColumn("step1_key", lit(null).cast("string"))
      .withColumn("step3_key", lit(null).cast("string"))
      .withColumn("step2_key", lit(null).cast("string"))
      .select(
        col("txn.account_number"),
        col("txn.holding_branch_key"),
        col("pp.product_type_code"),
        col("pp.relation_type_cd"),
        col("txn.opp_account_number"),
        col("txn.opp_branch_key"),
        col("txn.operation_type"),
        col("txn.ecif_key"),
        col("txn.card_number_trim").alias("card_number"),
        col("txn.execution_local_date_time"),
        col("txn.cr_dr_code"),
        col("txn.customer1_Account_holder_cif_id"),
        col("txn.customer2_Account_holder_cif_id"),
        col("pp.party_key").as("step4_key"),
        col("step1_key"),
        col("step2_key"),
        col("step3_key")
      )


    // Combine all steps into final output
    val joinedDF = step1
      .union(step2)
      .union(step3)
      .union(step4)


    val joinedDF1 = joinedDF.withColumn("conductor_key", coalesce(
        col("step1_key"),
        col("step2_key"),
        col("step3_key"),
        col("step4_key")
      ))
      .withColumn("on_behalf_of_ind", when(col("relation_type_cd").isin("11", "40"), "Y").otherwise("N"))
      .withColumn("third_party_cif_id", when(col("on_behalf_of_ind").isin("Y"), col("customer1_Account_holder_cif_id")).otherwise(lit(null)))
      .withColumn("primary_party_key", when(col("ecif_key").isNotNull, col("ecif_key")).otherwise(lit("null")))
      .withColumn("creditor_id", when(col("cr_dr_code").isin("D"), col("customer2_Account_holder_cif_id")).when(col("cr_dr_code").isin("C"), col("customer1_Account_holder_cif_id")).otherwise(lit("null")))

    joinedDF1

  }


}