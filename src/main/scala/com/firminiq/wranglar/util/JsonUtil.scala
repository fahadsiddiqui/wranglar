package com.firminiq.wranglar.util


import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.slf4j.LoggerFactory

object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val logger = LoggerFactory.getLogger(this.getClass)
  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJsonOrderMap(value: scala.collection.immutable.ListMap[String, Any]): String = {
    toJson(value map { case (k,v) => Symbol(k).name -> v})
  }
  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def getObjectMapper(): ObjectMapper ={
    this.mapper;
  }
  def toJsonFailedOnEmpty(value: Any): String = {

    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    mapper.setSerializationInclusion(Include.NON_NULL)
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

    mapper.writeValueAsString(value)
  }
  def toJsonwithoutNull(value: Any): String = {

    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    mapper.setSerializationInclusion(Include.NON_NULL)

    mapper.writeValueAsString(value)
  }
  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json)
  }

  def decodeSpecialCharsInJson(jsonString: String): String = {
    jsonString.replaceAll("&lt;", "<").replaceAll("&gt;", ">")
      .replaceAll("&amp;","&")
  }

}
