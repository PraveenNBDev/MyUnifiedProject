import MockData.{mockCddprfRskScoreLatestFull, mockEsdlPartyXRef, mockEsdlUnifiedTxn}
import org.apache.spark.sql.SparkSession


object MyUnifiedTxn extends App{

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
  val EsdlPartyProdDs = Seq(partyProd).toDS()
  val EsdlAccOpenDateDs = Seq(accOpenDate).toDS()
  val EsdlRefDs = Seq(ref,ref1).toDS()

  val esdlAccountDs = accDateDs.toDS()

  val dfEsdlUnifiedTxn = mockEsdlUnifiedTxn.toDS()
  val dfEsdlPartyXRef = mockEsdlPartyXRef.toDS()
  val dfCddprfRskScoreLatestFull = mockCddprfRskScoreLatestFull.toDS()

  TransformData.getEsdlTxnMapping(esdlTransactionDs, esdlAccountDs, EsdlAccOpenDateDs)
  TransformData.getUnifiedAttMap(dfEsdlUnifiedTxn, EsdlPartyProdDs, dfEsdlPartyXRef)
  TransformData.getEcifKeyAttMap(dfEsdlUnifiedTxn, EsdlPartyProdDs,EsdlAccOpenDateDs, dfCddprfRskScoreLatestFull)
  TransformData.getCust1AccHolderCifId(dfEsdlUnifiedTxn, EsdlPartyProdDs,EsdlAccOpenDateDs)

}
