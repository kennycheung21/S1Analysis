package com.hortonworks.support.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * Created by kzhang on 5/16/17.
  */
class CaseData(spark: SparkSession, p: Params) extends Serializable {
  val NOT_USE_AMBARI: String = "Not used"

  val file: String = p.file
  val table: String = p.table

  val overwrite: Boolean = p.overwrite

  val doc = new Documentation()

  @transient lazy private val logger = LogManager.getLogger(getClass)

  import spark.implicits._

  def loadCSV(): Dataset[InputDS] = {

    spark.sparkContext.register(doc, "Case_Documentation_Issues")

    val schema = caseDataCSV.getSchema
    //Construct InputDS based on input csv schema
    val csvDS = spark.read.schema(schema).option("header", "true").csv(file).select(col("Severity").as("severity"),
      col("Case Number").as("caseNum"), col("Status"), col("Date/Time Opened").as("dateOpen"), col("Date/Time Closed").as("dateClose"),
      col("Account Name").as("account"), col("Customer Record ID").as("accountID"), col("Ambari Version").as("ambariVersion"), col("Stack Version").as("stackVersion"),
      col("Product Component").as("component"), col("Hortonworks BugID URL").as("hBug"), col("Apache BugID URL").as("aBug"),
      col("HOTFIX BugID URL").as("hotfix"), col("EAR URL").as("EAR"), col("Root Cause").as("rootcause"), col("Enhancement Request Number").as("RMP"), col("Environment").as("Env"),
      col("Resolution Time").as("ResolutionTime")
    ).as[InputDS]

    csvDS
  }

  //Not efficient, should be process by row
  def updateDimentions(csvDS: Dataset[InputDS]): Tuple4[ Map[String, Int], Map[String, Int], Map[String, Int], Map[String, Int]] = {
    logger.info("Start to detect any new Ambari version, Stack version, customer, components and Root Cause.")

    val aVersion: Array[String] = csvDS.map(_.ambariVersion).distinct().collect().transform { s => if (s == null) NOT_USE_AMBARI else s }.toArray

    logger.debug(s"Total number of Ambari versions involved this week is ${aVersion.length}")

    val sVersion = csvDS.map(_.stackVersion).distinct().collect()

    logger.debug(s"Total number of Stack versions involved this week is ${sVersion.length}")

    val account: Map[Int, String] = csvDS.map { i: InputDS => (getAccountID(i.accountID).getOrElse(-1), i.account) }.distinct().collect().toMap[Int, String]

    logger.debug(s"Total number of customer involved this week is ${account.size}")

    val product: Array[String] = csvDS.map(_.component).distinct().collect()

    logger.debug(s"Total number of Components involved this week is ${product.length}")

    val rc: Array[String] = csvDS.map(_.rootcause).distinct().collect()

    logger.debug(s"Total number of Root Cause involved this week is ${rc.length}")

    val ambariVersionMap = spark.sql("select " + VersionRow.toSQLColumns + " from ambariVersion").flatMap { r: Row =>
      Some(r(0).toString, r(1).asInstanceOf[Int])
    }.collect().toMap[String, Int]

    logger.debug(s"Total number of Ambari version in our database is ${ambariVersionMap.size}")

    val stackVersionMap = spark.sql("select " + VersionRow.toSQLColumns + " from stackVersion").flatMap { r: Row =>
      Some(r(0).toString, r(1).asInstanceOf[Int])
    }.collect().toMap[String, Int]

    logger.debug(s"Total number of Stack version in our database is ${stackVersionMap.size}")

    val accountMap = spark.sql("select " + AccountRow.toSQLColumns + " from account").as[AccountRow].flatMap { r: AccountRow => Some((r.id, r.name)) }.collect().toMap[Int, String]

    logger.debug(s"Total number of Customer in our database is ${accountMap.size}")

    val productMap = spark.sql("select " + ProductRow.toSQLColumns + " from product").as[ProductRow].flatMap { r: ProductRow =>
      Some((r.name, r.id))
    }.collect().toMap[String, Int]

    logger.debug(s"Total number of Components in our database is ${productMap.size}")

    val rcMap = spark.sql("select " + RCRow.toSQLColumns + " from rootcause").as[RCRow].flatMap { r: RCRow =>
      Some((r.name, r.id))
    }.collect().toMap[String, Int]

    logger.debug(s"Total number of Root Cause in our database is ${rcMap.size}")

    val newAmbariMap: Map[String, Int] = updateVersionIfAny(aVersion, ambariVersionMap)
    val newStackMap: Map[String, Int] = updateVersionIfAny(sVersion, stackVersionMap)
    val newAccountMap: Map[Int, String] = updateAccountIfAny(account, accountMap)
    val newProductMap: Map[String, Int] = updateVersionIfAny(product, productMap)
    val newRCMap: Map[String, Int] = updateVersionIfAny(rc, rcMap)

    val newAmbari = if (newAmbariMap.size > ambariVersionMap.size) true else false
    val newStack = if (newStackMap.size > stackVersionMap.size) true else false
    val newAccount = if (newAccountMap.size > accountMap.size) true else false
    val newProduct = if (newProductMap.size > productMap.size) true else false
    val newRC = if (newRCMap.size > rcMap.size) true else false

    if (newAmbari) {
      logger.info("Detected and Adding new Ambari versions...")
      val ds = spark.createDataset(newAmbariMap.toSeq.map(a => VersionRow(a._1, a._2)))
      ds.select("version", "id").write.mode("overwrite").saveAsTable("ambariVersion")
    }

    if (newStack) {
      logger.info("Detected and Adding new Stack Versions...")
      val ds = spark.createDataset(newStackMap.toSeq.map(a => VersionRow(a._1, a._2)))
      ds.select("version", "id").write.mode("overwrite").saveAsTable("stackVersion")
    }

    if (newAccount) { //accountMap is not needed in the downstream
      logger.info("Detected and Adding new Account...")
      val ds = spark.createDataset(newAccountMap.toSeq.map(a => AccountRow(a._1, a._2)))
      ds.select("id", "name").write.mode("overwrite").saveAsTable("account")
    }

    if (newProduct) {
      logger.info("Detected and Adding new Product Component...")
      val ds = spark.createDataset(newProductMap.toSeq.map(a => ProductRow(a._1, a._2)))
      ds.select("name", "id").write.mode("overwrite").saveAsTable("product")
    }

    if (newRC) {
      logger.info("Detected and Adding new Root Cause...")
      val ds = spark.createDataset(newRCMap.toSeq.map(a => RCRow(a._1, a._2)))
      ds.select("name", "id").write.mode("overwrite").saveAsTable("rootcause")
    }
    (newProductMap, newRCMap, newAmbariMap, newStackMap)
  }

  def loadFact(csvDS: Dataset[InputDS], newProductMap: Map[String, Int], newRCMap: Map[String, Int], newAmbariMap: Map[String, Int], newStackMap: Map[String, Int], overwrite: Boolean = false) = {
    val bProductMap = spark.sparkContext.broadcast(newProductMap)
    val bRcMap = spark.sparkContext.broadcast(newRCMap)
    val bAmbariVersionMap = spark.sparkContext.broadcast(newAmbariMap)
    val bStackVersionMap = spark.sparkContext.broadcast(newStackMap)

    val hiveDS = csvDS.map{in => normalize(in, bProductMap, bRcMap, bAmbariVersionMap, bStackVersionMap)}.map{out => toFinalDS(out)}

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") //needed for new partition

    if (overwrite){
      logger.info(s"Overwritting new case data to table: ${table}")
      hiveDS.write.mode("overwrite").insertInto(table)
    }else{
      logger.info(s"Appending new case data to table: ${table}")
      hiveDS.write.mode("append").insertInto(table)
    }

    logger.info(s"Successfully add case data for this week, have a nice weekend!")
  }

  def updateFact(csvDS: Dataset[InputDS], newProductMap: Map[String, Int], newRCMap: Map[String, Int], newAmbariMap: Map[String, Int], newStackMap: Map[String, Int]) = {
    val bProductMap = spark.sparkContext.broadcast(newProductMap)
    val bRcMap = spark.sparkContext.broadcast(newRCMap)
    val bAmbariVersionMap = spark.sparkContext.broadcast(newAmbariMap)
    val bStackVersionMap = spark.sparkContext.broadcast(newStackMap)

    val hiveDS = csvDS.map{in => normalize(in, bProductMap, bRcMap, bAmbariVersionMap, bStackVersionMap)}.map{out => toFinalDS(out)}

    hiveDS.cache()

    val partitionSet = getPartitionSet(hiveDS)

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") //Just in case

    partitionSet.foreach{ a =>
      logger.info(s"""Updating the partition year=${a.year}/week="${a.week}"""")
      val partitionDS = hiveDS.where(s"""year=${a.year} and week="${a.week}"""")
      updateFactPartition(a.year, a.week, partitionDS)
    }

    logger.info(s"Successfully update case data for this week, have a nice weekend!")
  }

  def getPartitionSet(hiveDS: Dataset[FinalDS]): Array[Partitions] = {
    val partitionSet = hiveDS.select(col("year"), col("week")).distinct().as[Partitions].collect()
    partitionSet
  }

  def updateFactPartition(year: Int, week: String, newDS: Dataset[FinalDS]) = {

    val newCaseNum = newDS.select("casenum").distinct().as[Int].collect()

    logger.info(s"Updating cases: ${newCaseNum.mkString(",")}")

    val origPartition = spark.sql("select " + FinalDS.toSQLColumns + s""" from ${table} where year=${year} and week="${week}"""").as[FinalDS]


    val finalDS = origPartition.filter(f => !newCaseNum.contains(f.caseNum)).union(newDS)

    finalDS.createOrReplaceTempView("updateS1")

    val query : String = s"""
              Insert OVERWRITE table ${table} PARTITION (year=${year}, week="${week}") select """ + OutputDS.toSQLColumns +
      s""" from updateS1 where year=${year} and week="${week}"
       """.stripMargin

    logger.debug("About to update Fact table with SQL query: " + query)

    spark.sql(query)
  }

  def updateVersionIfAny (in: Array[String], lookup: Map[String, Int]): Map[String, Int] = {
    var maxId:Int = if (lookup.isEmpty) 0 else lookup.values.max
    val resultMap = scala.collection.mutable.Map() ++ lookup

    in.foreach{
      s:String =>
        if (!resultMap.contains(s)) {
          maxId += 1
          resultMap += ((s, maxId))
        }
    }

    resultMap.toMap
  }

  def updateAccountIfAny (in: Map[Int, String], lookup: Map[Int, String]): Map[Int, String] = {
    val resultMap = scala.collection.mutable.Map() ++ lookup

    for ((id, name) <- in){
      if (!resultMap.contains(id)){
        resultMap += ((id, name))
      } else { //update the name
        if(resultMap(id) != name)
          resultMap(id) = name
      }
    }
    resultMap.toMap
  }

  def normalize (in: InputDS, bProductMap: Broadcast[Map[String, Int]],
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
    val status: Int = if (in.status.equalsIgnoreCase("Closed")) 0 else 1
    val dateOpen: Option[Timestamp] = getTimestamp(in.dateOpen, caseNum, false)
    val dateClose: Option[Timestamp] = getTimestamp(in.dateClose, caseNum, true)
    val accountID: Int = getAccountID(in.accountID).getOrElse(-1)
    val ambariVersion: Int =  {
      var version:String = if (in.ambariVersion == null) NOT_USE_AMBARI else in.ambariVersion
      if (bAmbariVersionMap.value.get(version).isEmpty) newAmbari(version) else bAmbariVersionMap.value.get(version).get
    }
    val stackVersion: Int = if (bStackVersionMap.value.get(in.stackVersion).isEmpty) newAccount(in.stackVersion) else bStackVersionMap.value.get(in.stackVersion).get
    val component : Int = if (bProductMap.value.get(in.component).isEmpty) newComponent(in.component) else bProductMap.value.get(in.component).get
    val hBug: Option[Int] = if (in.hBug == null) None else getHBug(in.hBug, caseNum)
    val aBug: Option[String] = if (in.aBug == null) None else getABug(in.aBug, in.component, caseNum)
    val hotfix: Option[Int] = if (in.hotfix == null) None else getHotFix(in.hotfix, caseNum)
    val EAR: Option[Int] = if (in.EAR == null) None else getEAR(in.EAR, caseNum)
    val rootcause: Int = if (bRcMap.value.get(in.rootcause).isEmpty) newRC(in.rootcause) else bRcMap.value.get(in.rootcause).get
    val rmp: Option[Int] = if (in.RMP == null) None else getRMP(in.RMP, caseNum)
    val env: Option[Int] = if (in.Env == null) None else getEnv(in.Env)
    val resolutionTime: Option[Int] = getResolutionTimeToMinute(in.ResolutionTime)

    if (in.rootcause.equalsIgnoreCase("Undetermined at this time") && status == 0){
      doc.add((caseNum, s"Case is closed with invalid Root Cause ${in.rootcause}."))
    } else if ((in.rootcause.equalsIgnoreCase("Working as Designed") || in.rootcause.equalsIgnoreCase("Use Case Advice")) && status == 0 && severity == 1){
      doc.add((caseNum, s"S1(Production Down) should NOT be caused by root cause: ${in.rootcause}."))
    } else if (in.rootcause.equalsIgnoreCase("Documentation Defect") && status == 0 && hBug.isEmpty){
      doc.add((caseNum, s"Case caused by ${in.rootcause} without any Doc BUG filed."))
    } else if(in.rootcause.equalsIgnoreCase("System Limitation") && status == 0 && rmp.isEmpty){
      doc.add((caseNum, s"Case caused by ${in.rootcause} without any RMP filed."))
    } else if (in.rootcause.equalsIgnoreCase("No Customer Response") && status == 0 && severity == 1) {
      doc.add((caseNum, s"Is this a real S1 (Production Down) with No Customer Response?"))
    } else if (in.rootcause.equalsIgnoreCase("Product Defect") && status == 0 && (hBug.isEmpty && aBug.isEmpty)){
      doc.add((caseNum, s"Case is closed with Root Cause ${in.rootcause}, but there is no proper BUG documented."))
    }

    OutputDS(severity, caseNum,status,dateOpen,dateClose,accountID,ambariVersion,stackVersion,component,hBug,aBug,hotfix,EAR,rootcause,rmp, env, resolutionTime)
  }

  def getTimestamp(s: String, caseNum: Int, ignoreNull : Boolean = true) : Option[Timestamp] = s match {
    case "" =>
      if (!ignoreNull){
        logger.error(s"Open Date is not available for case ${caseNum}, data is wrong.")
        None
      }
      else {
        logger.debug(s"Close date for case ${caseNum} is not available yet.")
        None
      }
    case _ => {
      // 20170306 064703.000
      val format1 = new SimpleDateFormat("yyyyMMdd' 'HHmmss")

      //6/9/17 13:09
      val format2 = new SimpleDateFormat("MM/dd/yy' 'HH:mm")
      Try(new Timestamp(format1.parse(s).getTime)) match {
        case Success(t) =>
          logger.debug(s"Successfull get timestamp for case ${caseNum} "+ s + " => " + t)
          Some(t)
        case Failure(e) =>
          logger.warn(s"Fail to get timestamp in case ${caseNum} for ${s} with exception ${e}, date format need to follow: ${format1.toPattern}.")
          logger.info(s"Trying to parse the timestamp using different time format: ${format2.toPattern}...")
          Try(new Timestamp(format2.parse(s).getTime)) match  {
            case Success(t) =>
              logger.debug(s"Successfull get timestamp for case ${caseNum} "+ s + " => " + t)
              Some(t)
            case Failure(e) =>
              logger.warn(s"Fail to get timestamp in case ${caseNum} for ${s} with exception ${e}, date format need to follow: ${format2.toPattern}.")
              logger.error(s"Exhausted to get the timestamp using all acceptable format.")
              None
          }
      }
    }
  }

  def extractLastPart (s: String, caseNum: Int, backTrackLength: Int = -1): Option[String] = {
    val i = s.lastIndexOf("-")

    if (i < 0){
      doc.add((caseNum, s"Invalid JIRA URL: ${s}"))
      return None
    }else if (backTrackLength < 0){
      val slash = s.substring(0,i).lastIndexOf("/")
      return Some(s.substring(slash+1).trim.stripSuffix(";").stripSuffix("/").toUpperCase)
    }else if (i<backTrackLength){
      logger.warn(s"Invalid back track length for case ${caseNum}: ${s}")
      return None
    }


    Some(s.substring(i-backTrackLength).trim.stripSuffix(";").stripSuffix("/").toUpperCase)
  }

  def getHBug (s: String, caseNum: Int): Option[Int] = {
    val last: Option[String] = extractLastPart(s, caseNum)
    val bug: Option[Int] = last match {
      case Some(st) => {
        val p = """(BUG-)?(\d+)""".r

        val rs = st.toUpperCase match {
          case p(_, num) => Some(num.toInt)
          case _ =>{
            logger.warn(s"Invalid Hortonworks Bug: ${st}")
            doc.add((caseNum, s"Invalid Hortonworks Bug: ${st}"))
            None
          }
        }
        rs
      }
      case None => {
        logger.warn(s"Invalid Hortonworks Bug URL: ${s}")
        doc.add((caseNum, s"Invalid Hortonworks Bug URL: ${s}"))
        None
      }
    }
    bug
  }

  def getABug (s: String, component: String, caseNum: Int): Option[String] = {
    val last: Option[String] = extractLastPart(s, caseNum)
    val bug: Option[String] = last match {
      case Some(st) => {
        val p = """([A-Z]*)-(\d+)""".r

        val rs = st.toUpperCase match {
          case p(c,num) => {
            if (! c.equalsIgnoreCase(component) ) {
              logger.warn(s"The Apache JIRA ${st} doesn't match the problematic component ${component}.")
              doc.add((caseNum, s"The Apache JIRA ${st} doesn't match the problematic component ${component}."))
              None
            }else
              Some(num)
          }
          case _ => {
            logger.warn(s"Invalid Apache JIRA: ${st}")
            doc.add((caseNum, s"Invalid Apache JIRA: ${st}"))
            None
          }
        }
        rs
      }
      case None => {
        logger.warn(s"Invalid Apache JIRA URL: ${s}")
        doc.add((caseNum, s"Invalid Apache JIRA URL: ${s}"))
        None
      }
    }
    bug
  }

  def getHotFix (s: String, caseNum: Int): Option[Int] = {
    val last: Option[String] = extractLastPart(s, caseNum)
    val hotfix: Option[Int] = last match {
      case Some(st) => {
        val p = """(HOTFIX)-(\d+)""".r

        val rs = st.toUpperCase match {
          case p(hotfix, num) => Some(num.toInt)
          case _ => {
            logger.warn(s"Invalid Hortonworks Hotfix: ${st}")
            doc.add((caseNum, s"Invalid Hortonworks Hotfix: ${st}"))
            None
          }
        }
        rs
      }
      case None => {
        logger.warn(s"Invalid Hortonworks Hotfix URL: ${s}")
        doc.add((caseNum, s"Invalid Hortonworks Hotfix URL: ${s}"))
        None
      }
    }
    hotfix
  }

  def getEAR (s: String, caseNum: Int): Option[Int] = {
    val last: Option[String] = extractLastPart(s, caseNum)
    val ear: Option[Int] = last match {
      case Some(st) => {
        val p = """(EAR-)?(\d+)""".r

        val rs = st.toUpperCase match {
          case p(_, num) => Some(num.toInt)
          case _ => {
            logger.warn(s"Invalid Hortonworks EAR: ${st}")
            doc.add((caseNum, s"Invalid Hortonworks EAR: ${st}"))
            None
          }
        }
        rs
      }
      case None => {
        logger.warn(s"Invalid Hortonworks EAR URL: ${s}")
        doc.add((caseNum, s"Invalid Hortonworks EAR URL: ${s}"))
        None
      }
    }
    ear
  }

  def getRMP (s: String, caseNum: Int): Option[Int] = {
    val last: Option[String] = extractLastPart(s, caseNum)
    val rmp: Option[Int] = last match {
      case Some(st) => {
        val p = """(RMP-)?(\d+)""".r

        val rs = st.toUpperCase match {
          case p(_, num) => Some(num.toInt)
          case _ => {
            logger.warn(s"Invalid Hortonworks RMP: ${st}")
            doc.add((caseNum, s"Invalid Hortonworks RMP: ${st}"))
            None
          }
        }
        rs
      }
      case None => {
        logger.warn(s"Invalid Hortonworks RMP URL: ${s}")
        doc.add((caseNum, s"Invalid Hortonworks RMP URL: ${s}"))
        None
      }
    }
    rmp
  }

  def getEnv (s: String): Option[Int] = {
    if (s.isEmpty)
      return None
    val rs = s.substring(s.lastIndexOf("-")+1).toInt
    Some(rs)
  }

  def getAccountID (s: String): Option[Int] = {
    if (s.isEmpty) {
      logger.error("Unexpected empty Customer Record ID.")
      return None
    }
    val rs = s.substring(s.lastIndexOf("-")+1).toInt
    Some(rs)
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


  def toFinalDS(out: OutputDS) : FinalDS = {

    //workaround for Spark-20771
    val openDate = out.dateOpen

    if (openDate.isEmpty){
      logger.warn(s"Open date of Case ${out.caseNum} is empty, use the current date as workaround.")
    }

    val cal = Calendar.getInstance()

    cal.setTime(openDate.getOrElse(cal.getTime))

    cal.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY)

    val year = cal.getWeekYear

    val finalFormat = new SimpleDateFormat("MM-dd")
    val week = finalFormat.format(cal.getTime)

    logger.info(s"Case ${out.caseNum} belongs to partition year=${year} / week = ${week}")

    FinalDS(out.severity, out.caseNum, out.status, out.dateOpen, out.dateClose, out.accountID, out.ambariVersionID, out.stackVersionID, out.componentID,
      out.hBug, out.aBug, out.hotfix, out.EAR, out.rootcauseID, out.RMP, out.Env, out.ResolutionTime, year, week)
  }

  def getResolutionTimeToMinute(in: String): Option[Int] = {
    // 0 days 8 hours48 mn => 0 * 1440 + 8 * 60 + 48 * 1

    val rs: Option[Int] = if (in == null){
      None
    } else {
      val dayToMin: Int = 60 * 24
      val hourToMin: Int = 60

      val day_string = "days"
      val hour_string = "hours"
      val minute_string = "mn"

      val dayIndex = in.indexOf(day_string)
      val hourIndex = in.indexOf(hour_string)
      val minuteIndex = in.indexOf(minute_string)

      val days: Int = in.substring(0, dayIndex).trim.toInt
      val hours: Int = in.substring(dayIndex + day_string.length, hourIndex).trim.toInt
      val minutes: Int = in.substring(hourIndex + hour_string.length, minuteIndex).trim.toInt

      Some(days * dayToMin + hours * hourToMin + minutes)
    }

    rs
  }

  def wrapUp() = {
    import scala.collection.JavaConverters._

    val docMap = doc.value.asScala.toMap

    if (docMap.isEmpty)
      println("No Documentation issue Found! Perfect!")
    else{
      val number = docMap.size
      val startLine = s"===============================  Documentation issue(s) found in ${number} case(s) =============================="
      println(startLine)

      docMap.foreach{ en =>
        val st = s"Case ${en._1}    "
        val commentSet = en._2

        println(st + "  |  " + commentSet.head)

        val rest = commentSet.drop(1)

        if (!rest.isEmpty){
          val space: String = " " * st.length
          rest.foreach(c => println(space + "  |  " + c))
        }

        val lineLen = startLine.length

        val bottomLine = "-" * lineLen

        println(bottomLine)
      }
    }

    val src = p.file

    val cal = Calendar.getInstance()

    val finalFormat = new SimpleDateFormat("yyyyMMdd")

    val date = finalFormat.format(cal.getTime)

    val dst = src.substring(0,src.lastIndexOf(".")) + "_" + date + "_SUCCESS.csv"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    Try(fs.rename(new org.apache.hadoop.fs.Path(src), new org.apache.hadoop.fs.Path(dst))) match {
      case Success(t) =>
        logger.debug(s"Successfull rename file from ${src} to ${dst}")
      case Failure(e) =>
        logger.error(s"Failed to rename file from ${src} to ${dst}: " + e)
    }


  }
}
