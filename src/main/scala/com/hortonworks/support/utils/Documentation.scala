package com.hortonworks.support.utils

/**
  * Created by kzhang on 6/19/17.
  *
  * For document quality
  */

import java.util.concurrent.ConcurrentHashMap

import org.apache.log4j.LogManager
import org.apache.spark.util.AccumulatorV2

import scala.collection.JavaConverters._
import scala.collection.immutable.Set
import scala.collection.mutable.HashMap

class Documentation extends AccumulatorV2[(Int, String), ConcurrentHashMap[Int, Set[String]]] {
  @transient lazy private val logger = LogManager.getLogger(getClass)

  private val docMap : HashMap[Int, Set[String]] = HashMap()

  private val synMap = new ConcurrentHashMap[Int, Set[String]]()

  def add(k: Int, vSet: Set[String]) : Unit = {
    if (!docMap.contains(k)) {
      docMap.put(k, vSet)
    } else {
      val values = docMap.getOrElse(k, vSet).union(vSet)
      docMap.update(k, values)
    }
  }

  def add(kv: (Int, String)) = {
    if (!docMap.contains(kv._1)) {
      docMap.put(kv._1, Set(kv._2))
    } else {
      val values = docMap.getOrElse(kv._1, Set(kv._2))
      docMap.update(kv._1, values + kv._2)
    }
  }

  def copy(): Documentation = {
    val copied = new Documentation()

    docMap.foreach(en => copied.add(en._1, en._2))

    copied
  }

  def isZero: Boolean = this.docMap.isEmpty

  def merge(other: AccumulatorV2[(Int, String), ConcurrentHashMap[Int, Set[String]]]) = {
    val otherMap = other.value.asScala
    otherMap.foreach(en => this.add(en._1, en._2))
  }

  def reset() = {
    docMap.clear()
  }

  def value: ConcurrentHashMap[Int, Set[String]] = {
    synMap.clear()
    docMap.foreach(en => synMap.put(en._1, en._2))
    synMap
  }

}

