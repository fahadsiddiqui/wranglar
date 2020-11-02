package com.firminiq.wranglar.job

import com.firminiq.wranglar.business.InstructionProcessor
import com.firminiq.wranglar.util.JsonUtil._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.io.Source

object WranglerJob {
  val log = LoggerFactory.getLogger(getClass)
  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {

    if (args.size < 1) {
      System.err.println(s"Usage: spark-submit --class com.firminiq.wranglar.job.WranglerJob  --master yarn <locationof Jar> <JsonInstructions>")
    }
    val fileContents = Source.fromFile(args(0)).getLines.mkString
    val instructionJson = decodeSpecialCharsInJson(fileContents)

    log.info(s"instruction recived by Job : $instructionJson" )

    val spark = SparkSession.builder().
                appName(this.getClass.getName).
                getOrCreate()


    val processor = InstructionProcessor()

    processInstructions(instructionJson,spark,processor)
  }

private def processInstructions(instructionJson:String, spark:SparkSession,processor:InstructionProcessor):Unit = {
  val instructions = parse(instructionJson)
  val tableDataFrameMap = scala.collection.mutable.Map[String, DataFrame]()
  implicit val formats = DefaultFormats
  instructions.children map { x => processor.init(x, spark,tableDataFrameMap) }
  }
}
