import MockData.{mockCddprfRskScoreLatestFull, mockEsdlPartyXRef, mockEsdlUnifiedTxn}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}


object MyUnifiedTxn extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("spark").setLevel(Level.ERROR)
  Logger.getRootLogger().setLevel(Level.ERROR)
  Logger.getRootLogger.setLevel(Level.OFF)


  val spark = SparkSession.builder()
    .appName("Mock Data Example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val transaction = MockData.mockEsdlTransaction
  val amlReport = MockData.mockStgCertPayAmlReport
  val partyProd = MockData.mockEsdlPartyProd
  val accOpenDate = MockData.mockEsdlAccOpenDate
  val ref = MockData.mockEsdlRef
  val ref1 = MockData.mockEsdlRef1
  val config = MockData.mockPrmConfig
  val accDateDs = MockData.esdlAccountData

  val esdlTransactionDs = Seq(transaction).toDS()
  val StgCertPayAmlReport = Seq(amlReport).toDS()
  val esdlPartyProdDs = Seq(partyProd).toDS()
  val esdlAccOpenDateDs = Seq(accOpenDate).toDS()
  val EsdlRefDs = Seq(ref,ref1).toDS()

  val esdlAccountDs = accDateDs.toDS()

  val dfEsdlUnifiedTxn = mockEsdlUnifiedTxn.toDS()
  val dfEsdlPartyXRef = mockEsdlPartyXRef.toDS()
  val dfCddprfRskScoreLatestFull = mockCddprfRskScoreLatestFull.toDS()


  // Execute all transformations
  val transformations = Map(
    "esdlTxnMapping" -> TransformData.getEsdlTxnMapping(esdlTransactionDs, esdlAccountDs, esdlAccOpenDateDs),
    "unifiedAttMap" -> TransformData.getUnifiedAttMap(dfEsdlUnifiedTxn, esdlPartyProdDs, dfEsdlPartyXRef),
    "ecifKeyAttMap" -> TransformData.getEcifKeyAttMap(dfEsdlUnifiedTxn, esdlPartyProdDs, esdlAccOpenDateDs, dfCddprfRskScoreLatestFull),
    "cust1AccHolderCifId" -> TransformData.getCust1AccHolderCifId(dfEsdlUnifiedTxn, esdlPartyProdDs, esdlAccOpenDateDs),
    "accHoldersAll" -> TransformData.getAccHoldersAll(dfEsdlUnifiedTxn, esdlPartyProdDs, esdlAccOpenDateDs),
    "customer2AccountHolderCifId" -> TransformData.getCustomer2AccountHolderCifId(dfEsdlUnifiedTxn, esdlPartyProdDs, esdlAccOpenDateDs),
    "customer2AccountStatus" -> TransformData.getCustomer2AccountStatus(esdlTransactionDs, esdlAccountDs, esdlAccOpenDateDs),
    "customer2AccountCurrencyCode" -> TransformData.getCustomer2AccountCurrencyCode(esdlTransactionDs, esdlAccountDs, esdlAccOpenDateDs),
    "oppProdTypeResult" -> TransformData.calculateOppProdType(esdlPartyProdDs, esdlTransactionDs, esdlAccOpenDateDs)
  )

  // Join all results into a final unified dataset
  val finalResult = TransformData.joinAllResults(transformations)

  // Show and save results
  println("Final Unified Transaction Results:")
  val UnifiedDs = finalResult.select(
    col("txn_id1").alias("txn_id"),
    col("card_number21").alias("card_number"),
    col("account_number2").alias("account_number"),
    col("holding_branch_key2").alias("holding_branch_key"),
    col("account_key"),
    col("source_system_cd"),
    col("channel_cd"),
    col("source_transaction_id"),
    col("execution_local_date_time"),
    col("posting_Date"),
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
    col("ecif_composite_key3").alias("ecif_composite_key"),
    col("primary_party_key"),
    col("conductor_key"),
    col("ecif_key"),
    col("ip_address"),
    col("orph_ind"),
    col("orig_process_date"),
    col("loaded_to_cerebro"),
    col("loaded_to_hunter"),
    col("rules_cash_cad_equivalent_amt"),
    col("operation_type"),
    col("acct_holders_all"),
    col("txn_status_code"),
    col("txn_response_cd"),
    col("rules_cad_equivalent_amt"),
    col("emt_transfer_id"),
    col("emt_recipient_id"),
    col("recipient_sms"),
    col("sender_email"),
    col("processing_Date"),
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
    col("currency_conversion_rate"),
    col("user_id"),
    col("user_device_type"),
    col("user_session_date_time"),
    col("transaction_memo_line_1"),
    col("client_ip_addr"),
    col("debtor_id"),
    col("creditor_id"),
    col("addl_field_10"),
    col("fx_rate_exchange_rate"),
    col("addl_field_7"),
    col("addl_field_8"),
    col("addl_field_9"),
    col("organization_unit_cd"),
    col("opp_organization_unit_cd"),
    col("customer1_account_holder_cif_id"),
    col("customer2_account_holder_cif_id"),
    col("customer1_account_status"),
    col("customer2_account_status"),
    col("customer2_account_currency_code"),
    col("opp_prod_type"),
    col("fx_tran_exchange_rate"),
    col("on_behalf_of_ind"),
    col("third_party_cif_id"),
    col("txn.jnt_act_holder")
  )

  UnifiedDs.show()



}
