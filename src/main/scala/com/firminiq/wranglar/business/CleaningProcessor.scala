package com.firminiq.wranglar.business

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import  com.firminiq.wranglar.util.DataFrameUtil._

object CleaningProcessor {
  def apply() = new CleaningProcessor()
}




class CleaningProcessor {
  val log = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * @param df
    * @return df
    * Take df and trims all Alpahnumeric fields
    */
  def trimAlphaNumericFields(df:DataFrame):DataFrame = {
    val alphaNumericFields = getByType(df,"string")

    val trimDf = alphaNumericFields
                .foldLeft(df) { (memoryDF, colName) =>
                  memoryDF.withColumn(colName, trim(col(colName)))
              }

    trimDf

  }

  def replaceNullWithSlashN(df:DataFrame):DataFrame = {
    val replaceMap = df.schema.fieldNames.map(x=> (x,"\\N")).toMap
    df.na.fill(replaceMap)
  }

}

case class Column(name:String, dataType:String)
