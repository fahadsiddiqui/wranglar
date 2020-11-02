package com.firminiq.wranglar.job

import java.util.UUID

import com.firminiq.wranglar.business.InstructionProcessor
import com.firminiq.wranglar.util.JsonUtil.decodeSpecialCharsInJson
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.io.Source

class WranglerStructuredKafka {
  val log = LoggerFactory.getLogger(getClass)
  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println("Usage: WranglerStructuredKafka <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]  jobInstructions NameOfJob")
      System.exit(1)
    }


    val Array(bootstrapServers, subscribeType, topics, _*) = args

    val jobName = if (args.length >= 5) args(4) else "WranglerStructuredKafka" + UUID.randomUUID.toString

    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString


    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .getOrCreate()

    import spark.implicits._


    val fileContents = Source.fromFile(args(4)).getLines.mkString
    val instructionJson = decodeSpecialCharsInJson(fileContents)


    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    //TODO  THIS  has replace by Read  CSV, AVRO and JSON
    //TODO call processor.init() method

    val processor = InstructionProcessor()

    processInstructions(instructionJson,spark,processor)

    //  TODO replace line from   ( Save DF )
    val query = lines.writeStream
      .outputMode("complete")
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers) //TODO replace  by  save  method
      .option(subscribeType, topics)
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()

   }


  private def processInstructions(instructionJson:String, spark:SparkSession,processor:InstructionProcessor):Unit = {
    val instructions = parse(instructionJson)
    val tableDataFrameMap = scala.collection.mutable.Map[String, DataFrame]()
    implicit val formats = DefaultFormats
    instructions.children map { x => processor.init(x, spark,tableDataFrameMap) }
  }

  }
