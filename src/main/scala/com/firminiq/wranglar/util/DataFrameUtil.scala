package com.firminiq.wranglar.util

import org.apache.spark.sql.{DataFrame, functions}
import org.slf4j.LoggerFactory

object DataFrameUtil {

  val log = LoggerFactory.getLogger(this.getClass)

  def getPartitionSize(df: DataFrame): Int = {
    val coalesceSize = Math.min(Math.max(1, df.rdd.partitions.length/2), 200)
    coalesceSize
  }

  def getSchema(df: DataFrame):List[(String,String)] = {
    df.schema.fields.map(x => (getPhysicalDataType(x.dataType.typeName),x.name)).toList
  }

  /**
    *
    * @param df
    * @return
    */

  def getPhysicalDataType(df:DataFrame):Map[String,List[String]] = {
    getSchema(df).groupBy(_._1).mapValues(_.map(_._2))
  }

  /**
    *
    * @param df
    * @param dataType Physical data Type
    * @return
    */
  def getByType(df: DataFrame, dataType:String) :List[String] = {
    getPhysicalDataType(df).get(dataType.toLowerCase()).getOrElse(List())
  }


  def getPhysicalDataType(typ: String): String = {
    val isSpecial = typ.indexOf("(")
    val dataType = if (isSpecial>0) typ.substring(0,isSpecial) else typ
    val datTyp = dataType.toLowerCase() match {
      case "integer"    => "numeric"
      case "long"       => "numeric"
      case "double"     => "numeric"
      case "decimal"    => "numeric"
      case "float"      => "numeric"
      case "numeric"    => "numeric"
      case "byte"       => "numeric"
      case "tinyint"    => "numeric"
      case "int"        => "numeric"
      case "short"      => "numeric"
      case "bigdecimal" => "numeric"
      case "timestamp"  => "time"
      case "date"       => "date"
      case "boolean"    => "boolean"
      case _            => "string"
    }
    datTyp
  }


}
