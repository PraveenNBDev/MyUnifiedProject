import EsdlUnifiedTxnGenerator.generateUnifiedTxn
import MockData._
import Models._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object MyUnifiedTxn extends App {

  // Suppress Spark logs
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getRootLogger.setLevel(Level.ERROR)

  // Spark Session
  val spark = SparkSession.builder()
    .appName("ESDL Unified Transaction Processor")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Load all mock datasets
  val esdlTransactionDs = Seq(mockEsdlTransaction).toDS()
  val esdlPartyProdDs = Seq(mockEsdlPartyProd).toDS()
  val esdlAccOpenDateDs = Seq(mockEsdlAccOpenDate).toDS()
  val dfCddprfRskScoreLatestFull = mockCddprfRskScoreLatestFull.toDS()
  val esdlPartyXRefDs = mockEsdlPartyXRef.toDS()
  val esdlAccountDs = esdlAccountData.toDS()
  val esdlUnifiedTxnDs = mockEsdlUnifiedTxn.toDS()

  // Step 1: Generate Unified Txn
  val unifiedTxnDs = generateUnifiedTxn(
    spark,
    esdlTransactionDs,
    esdlPartyProdDs,
    esdlAccOpenDateDs,
    dfCddprfRskScoreLatestFull,
    esdlPartyXRefDs
  )

  // Derived transformations
  val esdlTxnMapping = TransformData.getEsdlTxnMapping(esdlTransactionDs, esdlAccountDs, esdlAccOpenDateDs)
  val accHoldersAll = TransformData.getAccHoldersAll(esdlUnifiedTxnDs, esdlPartyProdDs, esdlAccOpenDateDs)
  val customer2AccountStatus = TransformData.getCustomer2AccountStatus(esdlTransactionDs, esdlAccountDs, esdlAccOpenDateDs)
  val customer2AccountCurrencyCode = TransformData.getCustomer2AccountCurrencyCode(esdlTransactionDs, esdlAccountDs, esdlAccOpenDateDs)
  val oppProdTypeResult = TransformData.calculateOppProdType(esdlPartyProdDs, esdlTransactionDs, esdlAccOpenDateDs)

  // Derived unified txn attributes for joins
  val unifiedAttMap = esdlUnifiedTxnDs
    .select(col("account_number"), col("card_number"), col("operation_type"))

  val ecifKeyAttMap = esdlUnifiedTxnDs
    .select(col("account_number"), col("ecif_key"))

  val cust1AccHolderCifId = esdlUnifiedTxnDs
    .select(col("account_number"), col("customer1_Account_holder_cif_id"))

  val customer2AccountHolderCifId = esdlUnifiedTxnDs
    .select(col("opp_account_number"), col("opp_branch_key"), col("customer2_Account_holder_cif_id"))

  // Joining all DataFrames
  val joinedResults = esdlTxnMapping
    .join(unifiedAttMap,
      col("esdlTxnMapping.opp_account_number") === col("unifiedAttMap.account_number"),
      "left_outer")

    .join(ecifKeyAttMap,
      col("esdlTxnMapping.opp_account_number") === col("ecifKeyAttMap.account_number"),
      "left_outer")

    .join(cust1AccHolderCifId,
      col("esdlTxnMapping.account_number") === col("cust1AccHolderCifId.account_number"),
      "left_outer")

    .join(accHoldersAll,
      col("esdlTxnMapping.account_number") === col("accHoldersAll.account_number"),
      "left_outer")

    .join(customer2AccountHolderCifId,
      col("esdlTxnMapping.opp_account_number") === col("customer2AccountHolderCifId.opp_account_number") &&
        col("esdlTxnMapping.opp_branch_key") === col("customer2AccountHolderCifId.opp_branch_key"),
      "left_outer")

    .join(customer2AccountStatus,
      col("esdlTxnMapping.opp_account_number") === col("customer2AccountStatus.account_number") &&
        col("esdlTxnMapping.opp_branch_key") === col("customer2AccountStatus.holding_branch_key"),
      "left_outer")

    .join(customer2AccountCurrencyCode,
      col("esdlTxnMapping.account_number") === col("customer2AccountCurrencyCode.account_number") &&
        col("esdlTxnMapping.holding_branch_key") === col("customer2AccountCurrencyCode.holding_branch_key"),
      "left_outer")

    .join(oppProdTypeResult,
      col("esdlTxnMapping.opp_account_number") === col("oppProdTypeResult.account_number") &&
        col("esdlTxnMapping.opp_branch_key") === col("oppProdTypeResult.holding_branch_key"),
      "left_outer")

  // Show results
  println("Final Joined Unified Transaction Output:")
  joinedResults.show(truncate = false)

  // Stop Spark
  spark.stop()
}
