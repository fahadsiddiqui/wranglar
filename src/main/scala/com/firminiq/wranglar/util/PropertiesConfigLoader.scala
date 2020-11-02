package com.firminiq.wranglar.util
import org.slf4j.LoggerFactory
import java.io.{FileInputStream, StringReader}
import java.util.Properties

object PropertiesConfigLoader {
  val log = LoggerFactory.getLogger(this.getClass)

  def getPropertiesAsMap(path: String): Map[String, String] = {
    log.info("Loading properties from {}", path)
    val prop = new Properties()
    prop.load(new FileInputStream(path))
    if (prop.size() == 0){
      throw new RuntimeException(s"Cannot load $path, or the property file is empty")
    }
    import scala.collection.JavaConverters._
    prop.asScala.toMap
  }

  def getPropertiesFromString(content: String): Map[String, String] = {
    log.info("Loading properties from \n{}", content)
    val prop = new Properties()
    prop.load(new StringReader(content))
    if (prop.size() == 0){
      throw new RuntimeException(s"Cannot load $content, or the property file is empty")
    }
    import scala.collection.JavaConverters._
    prop.asScala.toMap
  }

}

object Config {
  val S3_KEY = "fs.s3n.awsAccessKeyId"
  val S3_SECRET = "fs.s3n.awsSecretAccessKey"

}
