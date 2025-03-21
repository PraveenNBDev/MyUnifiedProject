import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MyUnifiedTxn extends App{


  Logger.getLogger("org").setLevel(Level.ERROR)
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

  TransformData.getEsdlTxnMapping(esdlTransactionDs, esdlAccountDs, EsdlAccOpenDateDs)

}
