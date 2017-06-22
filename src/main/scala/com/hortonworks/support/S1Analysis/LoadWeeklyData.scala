package com.hortonworks.support.S1Analysis

import com.hortonworks.support.utils._
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
  * Created by kzhang on 3/13/17.
  */


object LoadWeeklyData {

  def main(args: Array[String]) {

    val defaultParams = Params("/tmp/S1/S1.csv", "S1")

    val spark = SparkSession.builder().appName("S1WeeklyLoad").enableHiveSupport().getOrCreate()

    val parser = new OptionParser[Params]("S1WeedklyLoad") {
      head("S1 Weekly Data load: Load the weekly S1 cases data")
      opt[String]('f', "file")
        .required()
        .text("The file path of S1 csv file in HDFS")
        .action((x, c) => c.copy(file = x.toString))
      opt[String]('t', "table")
        .required()
        .text("The Hive table to load data into")
        .action((x, c) => c.copy(table = x.toString))
      opt[String]('o', "overwrite")
        .text("Whether to overwrite the exsisting table/partitions")
        .action((x, c) => c.copy(overwrite = x.toBoolean))
      note(
        """
          |Usage: LoadWeeklyData.jar --file /tmp/S1/S1.csv -t S1 -o false
          |  <file> S1 Weekly Data load: Load the weekly S1 cases data [required]
          |  <table> The Hive Fact table to load data into [required]
          |  <overwrite> Whether to overwrite the exsisting table/partitions, false by default
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map {
      p => {
        println("Starting to load data with Params: \n" + p.toString)

        val caseData = new CaseData(spark, p)

        val csvDS = caseData.loadCSV()

        csvDS.cache()
        //This is to force to update dimentions first, then update fact
        val(newProductMap, newRCMap, newAmbariMap, newStackMap) = caseData.updateDimentions(csvDS)

        caseData.loadFact(csvDS, newProductMap, newRCMap, newAmbariMap, newStackMap, p.overwrite)

        caseData.wrapUp()
      }
    } getOrElse {
      System.exit(1)
    }
  }
}

/*class LoadWeeklyData (spark: SparkSession) extends Serializable {

  val NOT_USE_AMBARI: String = "Not used"

  @transient lazy private val logger = LogManager.getLogger(getClass)

  def run(p: Params): Unit ={

    val file: String = p.file
    val table: String = p.table

    val schema = StructType(Array(
      StructField("Severity", DataTypes.StringType),
      StructField("Customer Record ID", DataTypes.IntegerType),
      StructField("Account Name", DataTypes.StringType), // String => Int
      StructField("Case Number", DataTypes.IntegerType), // Int
      StructField("Status", DataTypes.StringType), //String => Int
      StructField("Date/Time Opened", DataTypes.StringType), //String => timestamp
      StructField("Date/Time Closed", DataTypes.StringType), //String => timestamp
      StructField("Ambari Version", DataTypes.StringType), //String => Int
      StructField("Stack Version", DataTypes.StringType), //String => Int
      StructField("Product Component", DataTypes.StringType), //String => Int
      StructField("Apache BugID URL", DataTypes.StringType), //String => Int
      StructField("Hortonworks BugID URL", DataTypes.StringType), //String => Int
      StructField("HOTFIX BugID URL", DataTypes.StringType), //String => Int
      StructField("EAR URL", DataTypes.StringType), //String => Int
      StructField("Root Cause", DataTypes.StringType), //String => Int
      StructField("Enhancement Request Number", DataTypes.StringType) //String => Int
    ))

    import spark.implicits._

    val csvDS = spark.read.schema(schema).option("header", "true").csv(file).select(col("Severity").as("severity"),
      col("Case Number").as("caseNum"), col("Status"), col("Date/Time Opened").as("dateOpen"), col("Date/Time Closed").as("dateClose"),
      col("Account Name").as("account"), col("Customer Record ID").as("accountID"), col("Ambari Version").as("ambariVersion"), col("Stack Version").as("stackVersion"),
      col("Product Component").as("component"), col("Hortonworks BugID URL").as("hBug"), col("Apache BugID URL").as("aBug"),
      col("HOTFIX BugID URL").as("hotfix"), col("EAR URL").as("EAR"), col("Root Cause").as("rootcause"), col("Enhancement Request Number").as("RMP")
      ).as[InputDS]

    csvDS.cache()

    logger.info("Start to detect any new Ambari version, Stack version, customer, components and Root Cause.")

    val aVersion: Array[String] = csvDS.map(_.ambariVersion).distinct().collect().transform{s => if (s == null) NOT_USE_AMBARI else s }.toArray

    logger.debug(s"Total number of Ambari versions involved this week is ${aVersion.length}")

    val sVersion = csvDS.map(_.stackVersion).distinct().collect()

    logger.debug(s"Total number of Stack versions involved this week is ${sVersion.length}")

    val account: Map[String, Int] = csvDS.map{i:InputDS => (i.account, i.accountID)}.distinct().collect().toMap[String, Int]

    logger.debug(s"Total number of customer involved this week is ${account.size}")

    val product: Array[String] = csvDS.map(_.component).distinct().collect()

    logger.debug(s"Total number of Components involved this week is ${product.length}")

    val rc: Array[String] = csvDS.map(_.rootcause).distinct().collect()

    logger.debug(s"Total number of Root Cause involved this week is ${rc.length}")

    val ambariVersionMap = spark.sql("select version, id from ambariVersion").flatMap{r: Row =>
      Some(r(0).toString, r(1).asInstanceOf[Int])
    }.collect().toMap[String, Int]

    logger.debug(s"Total number of Ambari version in our database is ${ambariVersionMap.size}")

    val stackVersionMap = spark.sql("select version, id from stackVersion").flatMap{r: Row =>
      Some(r(0).toString, r(1).asInstanceOf[Int])
    }.collect().toMap[String, Int]

    logger.debug(s"Total number of Stack version in our database is ${stackVersionMap.size}")

    val accountMap = spark.sql("select name, id from account").as[AccountRow].flatMap{r: AccountRow => Some((r.name, r.id))}.collect().toMap[String, Int]

    logger.debug(s"Total number of Customer in our database is ${accountMap.size}")

    val productMap = spark.sql("select name, id from product").flatMap{r: Row =>
      val seqRow = r.toSeq
      Some((seqRow(0).toString, seqRow(1).asInstanceOf[Int]))
    }.collect().toMap[String, Int]

    logger.debug(s"Total number of Components in our database is ${productMap.size}")

    val rcMap = spark.sql("select rootcause, id from rootcause").flatMap{r: Row =>
      val seqRow = r.toSeq
      Some((seqRow(0).toString, seqRow(1).asInstanceOf[Int]))
    }.collect().toMap[String, Int]

    logger.debug(s"Total number of Root Cause in our database is ${rcMap.size}")

    val newAmbariMap: Map[String, Int] = updateVersionIfAny(aVersion, ambariVersionMap)
    val newStackMap: Map[String, Int] = updateVersionIfAny(sVersion, stackVersionMap)
    val newAccountMap: Map[String, Int] = updateAccountIfAny(account, accountMap)
    val newProductMap: Map[String, Int] = updateVersionIfAny(product, productMap)
    val newRCMap: Map[String, Int] = updateVersionIfAny(rc, rcMap)

    val newAmbari = if (newAmbariMap.size > ambariVersionMap.size) true else false
    val newStack = if (newStackMap.size > stackVersionMap.size) true else false
    val newAccount = if (newAccountMap.size > accountMap.size) true else false
    val newProduct = if (newProductMap.size > productMap.size) true else false
    val newRC = if (newRCMap.size > rcMap.size) true else false

    if (newAmbari){
      logger.info("Detected and Adding new Ambari versions...")
      val ds = spark.createDataset(newAmbariMap.toSeq.map(a => VersionRow(a._1, a._2)))
      ds.select("version", "id").write.mode("overwrite").saveAsTable("ambariVersion")
    }

    if (newStack){
      logger.info("Detected and Adding new Stack Versions...")
      val ds = spark.createDataset(newStackMap.toSeq.map(a => VersionRow(a._1, a._2)))
      ds.select("version", "id").write.mode("overwrite").saveAsTable("stackVersion")
    }

    if (newAccount){
      logger.info("Detected and Adding new Account...")
      val ds = spark.createDataset(newAccountMap.toSeq.map(a => AccountRow(a._1, a._2)))
      ds.select("name", "id").write.mode("overwrite").saveAsTable("account")
    }

    if (newProduct){
      logger.info("Detected and Adding new Product Component...")
      val ds = spark.createDataset(newProductMap.toSeq.map(a => AccountRow(a._1, a._2)))
      ds.select("name", "id").write.mode("overwrite").saveAsTable("product")
    }

    if (newRC){
      logger.info("Detected and Adding new Root Cause...")
      val ds = spark.createDataset(newRCMap.toSeq.map(a => RCRow(a._1, a._2)))
      ds.select("rootcause", "id").write.mode("overwrite").saveAsTable("rootcause")
    }

    val bAccountMap = spark.sparkContext.broadcast(newAccountMap)
    val bProductMap = spark.sparkContext.broadcast(newProductMap)
    val bRcMap = spark.sparkContext.broadcast(newRCMap)
    val bAmbariVersionMap = spark.sparkContext.broadcast(newAmbariMap)
    val bStackVersionMap = spark.sparkContext.broadcast(newStackMap)

    val hiveDS = csvDS.map{in => normalize(in, bAccountMap, bProductMap, bRcMap, bAmbariVersionMap, bStackVersionMap)}.withColumn("year", year(col("dateOpen"))).withColumn("week", weekofyear(col("dateOpen")))

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") //needed

    logger.info(s"Saving case data to table: ${table}")
    hiveDS.write.insertInto(table)

    logger.info(s"Successfully add case data for this week, have a nice weekend!")
  }

  def updateVersionIfAny (in: Array[String], lookup: Map[String, Int]): Map[String, Int] = {
    var maxId:Int = if (lookup.isEmpty) 0 else lookup.values.max
    val resultMap = lookup.toBuffer

    in.foreach{
      s:String =>
        if (!lookup.contains(s)) {
          resultMap+=((s,maxId))
          maxId += 1
        }
    }

    resultMap.toMap
  }

  def updateAccountIfAny (in: Map[String, Int], lookup: Map[String, Int]): Map[String, Int] = {
    val resultMap = lookup.toBuffer

    for ((name, id) <- in){
      if (!lookup.contains(name))
        resultMap += ((name, id))
    }
    resultMap.toMap
  }

  def normalize (in: InputDS, bAccountMap: Broadcast[Map[String, Int]], bProductMap: Broadcast[Map[String, Int]],
                 bRcMap: Broadcast[Map[String, Int]], bAmbariVersionMap: Broadcast[Map[String, Int]],
                 bStackVersionMap: Broadcast[Map[String, Int]]): OutputDS ={

    val severity: Int = in.severity match {
      case "S1 - Production Down" => 1
      case "S2 - Core Feature Inoperative" => 2
      case "S3 - Minor Feature Inoperative" => 3
      case "S4 - Request for Information" => 4
      case _ => -1
    }
    val caseNum: Int = in.caseNum
    val status: Int = if (in.status == "Closed") 0 else 1
    val dateOpen: Option[Timestamp] = getTimestamp(in.dateOpen)
    val dateClose: Option[Timestamp] = getTimestamp(in.dateClose)
    val account: Int = if (bAccountMap.value.get(in.account).isEmpty) newAccount(in.account) else bAccountMap.value.get(in.account).get
    val ambariVersion: Int =  {
      var version:String = if (in.ambariVersion == null) NOT_USE_AMBARI else in.ambariVersion
      if (bAmbariVersionMap.value.get(version).isEmpty) newAmbari(version) else bAmbariVersionMap.value.get(version).get
    }
    val stackVersion: Int = if (bStackVersionMap.value.get(in.stackVersion).isEmpty) newAccount(in.stackVersion) else bStackVersionMap.value.get(in.stackVersion).get
    val component : Int = if (bProductMap.value.get(in.component).isEmpty) newComponent(in.component) else bProductMap.value.get(in.component).get
    val hBug: Option[Int] = if (in.hBug == null) None else getHBug(in.hBug)
    val aBug: Option[String] = if (in.aBug == null) None else getABug(in.aBug)
    val hotfix: Option[Int] = if (in.hotfix == null) None else getHotFix(in.hotfix)
    val EAR: Option[Int] = if (in.EAR == null) None else getEAR(in.EAR)
    val rootcause: Int = if (bRcMap.value.get(in.rootcause).isEmpty) newRC(in.rootcause) else bRcMap.value.get(in.rootcause).get
    val rmp: Option[Int] = if (in.RMP == null) None else getRMP(in.RMP)

    OutputDS(severity, caseNum,status,dateOpen,dateClose,account,ambariVersion,stackVersion,component,hBug,aBug,hotfix,EAR,rootcause,rmp)
  }

  def getTimestamp(s: String) : Option[Timestamp] = s match {
    case "" =>
      logger.debug("Null date.")
      None
    case _ => { // 1/20/2017  3:25:00 AM
    val format = new SimpleDateFormat("MM/dd/yy' 'KK:mm")
      Try(new Timestamp(format.parse(s).getTime)) match {
        case Success(t) =>
          logger.debug("Successfull get timestamp for "+ s + " => " + t)
          Some(t)
        case Failure(e) =>
          logger.error(s"Fail to get timestamp for ${s} with exception ${e}, date format need to follow: ${format}.")
          None
      }
    }
  }

  def extractLastPart (s: String): Option[String] = {
    val url = new URL(s)
    val file = url.getFile
    val temp = file.split("/")

    Some(temp(temp.length - 1).toUpperCase)
  }

  def getHBug (s: String): Option[Int] = {
    val last: Option[String] = extractLastPart(s)
    val bug: Option[Int] = last match {
      case Some(s) => {
        val p = """(BUG-)?(\d+)""".r

        val rs = s match {
          case p(_, num) => Some(num.toInt)
          case _ => None
        }
        rs
      }
      case None => None
    }
    bug
  }

  def getABug (s: String): Option[String] = {
    val last: Option[String] = extractLastPart(s)
    val bug: Option[String] = last match {
      case Some(s) => {
        val p = """([A-Z]*)(-)(\d+)""".r

        val rs = s match {
          case p(_*) => Some(s)
          case _ => None
        }
        rs
      }
      case None => None
    }
    bug
  }

  def getHotFix (s: String): Option[Int] = {
    val last: Option[String] = extractLastPart(s)
    val hotfix: Option[Int] = last match {
      case Some(s) => {
        val p = """(HOTFIX-)?(\d+)""".r

        val rs = s match {
          case p(_, num) => Some(num.toInt)
          case _ => None
        }
        rs
      }
      case None => None
    }
    hotfix
  }

  def getEAR (s: String): Option[Int] = {
    val last: Option[String] = extractLastPart(s)
    val ear: Option[Int] = last match {
      case Some(s) => {
        val p = """(EAR-)?(\d+)""".r

        val rs = s match {
          case p(_, num) => Some(num.toInt)
          case _ => None
        }
        rs
      }
      case None => None
    }
    ear
  }

  def getRMP (s: String): Option[Int] = {
    val last: Option[String] = extractLastPart(s)
    val rmp: Option[Int] = last match {
      case Some(s) => {
        val p = """(RMP-)?(\d+)""".r

        val rs = s match {
          case p(_, num) => Some(num.toInt)
          case _ => None
        }
        rs
      }
      case None => None
    }
    rmp
  }
  
  def newAccount(s: String): Int = {
    logger.error(s"Something wrong with this Account: ${s}, setting its id to -1")
    -1
  }

  def newComponent(s: String): Int = {
    logger.error(s"Something wrong with this Component: ${s}, setting its id to -1")
    -1
  }

  def newRC(s: String): Int = {
    logger.error(s"Something wrong with this Root Cause: ${s}, setting its id to -1")
    -1
  }

  def newAmbari(s: String): Int = {
    logger.error(s"Something wrong with this Ambari Version: ${s}, setting its id to -1")
    -1
  }

  def newStack(s: String): Int = {
    logger.error(s"Something wrong with this Stack Version: ${s}, setting its id to -1")
    -1
  }
}*/
