package com.hortonworks.support.utils

import java.sql.Timestamp

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by kzhang on 5/11/17.
  */

case class Params(file: String, table: String, overwrite: Boolean = false)  {
  override def toString: String = {
    val paramValue = "File Path: " + file + "\n"
    paramValue
  }
}
//18 columns as 5/17, the order matter
object caseDataCSV {
  val schema = StructType(Array(
    StructField("Severity", DataTypes.StringType),
    StructField("Customer Record ID", DataTypes.StringType),
    StructField("Account Name", DataTypes.StringType), // String => Int
    StructField("Case Number", DataTypes.IntegerType), // Int
    StructField("Status", DataTypes.StringType), //String => Int
    StructField("Date/Time Opened", DataTypes.StringType), //String => timestamp
    StructField("Date/Time Closed", DataTypes.StringType), //String => timestamp
    StructField("Resolution Time", DataTypes.StringType),  //String
    StructField("Ambari Version", DataTypes.StringType), //String => Int
    StructField("Stack Version", DataTypes.StringType), //String => Int
    StructField("Product Component", DataTypes.StringType), //String => Int
    StructField("Apache BugID URL", DataTypes.StringType), //String => Int
    StructField("Hortonworks BugID URL", DataTypes.StringType), //String => Int
    StructField("HOTFIX BugID URL", DataTypes.StringType), //String => Int
    StructField("EAR URL", DataTypes.StringType), //String => Int
    StructField("Root Cause", DataTypes.StringType), //String => Int
    StructField("Enhancement Request Number", DataTypes.StringType), //String => Int
    StructField("Environment", DataTypes.StringType) //String => Int
  ))

  def getSchema: StructType = return schema
}

case class InputDS(severity: String, caseNum: Int, status: String, dateOpen: String, dateClose: String, account: String, accountID: String,
                   ambariVersion: String, stackVersion: String, component: String, hBug: String,
                   aBug: String, hotfix: String, EAR: String, rootcause: String, RMP: String, Env: String, ResolutionTime: String) extends Serializable

case class OutputDS(severity: Int, caseNum: Int, status: Int, dateOpen: Option[Timestamp], dateClose: Option[Timestamp], accountID: Int,
                    ambariVersionID: Int, stackVersionID: Int, componentID: Int, hBug: Option[Int],
                    aBug: Option[String], hotfix: Option[Int], EAR: Option[Int], rootcauseID: Int, RMP: Option[Int], Env: Option[Int], ResolutionTime: Option[Int]) extends Serializable
object OutputDS {
  //Helper function in case of any schema udpate
  def toSQLColumns: String = {
    val cols: String = "severity, caseNum, status, dateOpen, dateClose, accountID, ambariVersionID, stackVersionID, componentID, hBug, aBug, hotfix, EAR, rootcauseID, RMP, Env, ResolutionTime"

    return cols
  }
}
//Add partition info based on output DS
case class FinalDS(severity: Int, caseNum: Int, status: Int, dateOpen: Option[Timestamp], dateClose: Option[Timestamp], accountID: Int,
                    ambariVersionId: Int, stackVersionId: Int, componentId: Int, hBug: Option[Int],
                    aBug: Option[String], hotfix: Option[Int], EAR: Option[Int], rootcauseId: Int, RMP: Option[Int], Env: Option[Int], ResolutionTime: Option[Int],
                   year: Int, week: String) extends Serializable
object FinalDS {
  def toSQLColumns: String = {
    val cols: String = "severity, caseNum, status, dateOpen, dateClose, accountID, ambariVersionID, stackVersionID, componentID, hBug, aBug, hotfix, EAR, rootcauseID, RMP, Env, ResolutionTime, year, week"

    return cols
  }
}

case class Partitions(year: Int, week: String) extends Serializable

case class AccountRow(id: Int, name: String) extends Serializable

object AccountRow {
  def toSQLColumns: String = {
    val cols: String = "id, name"

    return cols
  }
}

case class VersionRow(version: String, id: Int) extends Serializable

object VersionRow {
  def toSQLColumns: String = {
    val cols: String = "version, id"

    return cols
  }
}

case class ProductRow(name: String, id: Int) extends Serializable

object ProductRow {
  def toSQLColumns: String = {
    val cols: String = "name, id"

    return cols
  }
}

case class RCRow(name: String, id: Int) extends Serializable

object RCRow {
  def toSQLColumns: String = {
    val cols: String = "name, id"

    return cols
  }
}