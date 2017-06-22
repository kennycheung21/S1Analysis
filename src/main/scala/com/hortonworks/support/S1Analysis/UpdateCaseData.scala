package com.hortonworks.support.S1Analysis

import com.hortonworks.support.utils.{Params, _}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
  * Created by kzhang on 5/5/17.
  */

object UpdateCaseData {

  def main(args: Array[String]) {

    val defaultParams = Params("/tmp/S1/S1_update.csv", "S1")

    val spark = SparkSession.builder().appName("S1WeeklyUpdate").enableHiveSupport().getOrCreate()

    val parser = new OptionParser[Params]("S1WeedklyLoad") {
      head("S1 Weekly Data Update: Update the old S1 cases data")
      opt[String]('f', "file")
        .required()
        .text("The file path of S1_update csv file in HDFS")
        .action((x, c) => c.copy(file = x.toString))
      opt[String]('t', "table")
        .required()
        .text("The Hive table to update data in")
        .action((x, c) => c.copy(table = x.toString))
      note(
        """
          |Usage: LoadWeeklyData.jar --file /tmp/S1/S1.csv -t S1
          |  <file> S1 Weekly Data load: Load the weekly S1 cases data [required]
          |  <table> The Hive table to load data into [required]
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map {
      p => {
        println("Starting to update data with Params: \n" + p.toString)

        val caseData = new CaseData(spark, p)

        val csvDS = caseData.loadCSV()

        csvDS.cache()
        //Forced to update dimentions first
        val(newProductMap, newRCMap, newAmbariMap, newStackMap) = caseData.updateDimentions(csvDS)

        caseData.updateFact(csvDS, newProductMap, newRCMap, newAmbariMap, newStackMap)

        caseData.wrapUp()
      }
    } getOrElse {
      System.exit(1)
    }
  }
}
