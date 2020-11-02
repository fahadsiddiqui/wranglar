package com.firminiq.wranglar.domain

import com.firminiq.wranglar.domain.InstructionObject.{FileDescriptor, UserInformation}

object IngestionInstructionObject {
  case class KeyVal(key:String, value:String)
  case class Schema(
                     srcColumn: String,
                     destColumnName: Option[String],
                     srcDataType: String,
                     destDataType: Option[String],
                     format: Option[String],
                     scale: Option[String],
                     precision: Option[String]
                   )

  case class Column(originalColumnName:String, originalColumnType:String,
                    targetColumnName:Option[String], targetColumnType:Option[String], tags: List[String] = List())

  case class Target(dataBase: Option[String], tableName: String,
                    fileFormat: Option[String], filesPerPartition:Option[Int],
                    compression: Option[String] ,partitionCols: Option[List[String]]
                   )
  class SourceJDBC(dataBase: Option[String], tableName: String,hostname:String, port:Int,category:String ,// TODO Define END MYSQL ORACLE etc
                   incrementKey: Option[List[String]],properties: Option[List[KeyVal]],sourcePartition: Option[List[String]]
                 ,numberOfParallelTask:Option[Int])

  class SourceFile(fileDescriptor: FileDescriptor,storageSystem:String, //TODO  S3, ADLS etc
                   properties:List[KeyVal] // This  will have all connection properties
                  )
  class CommonForAll(selectColumn:List[String],
                     schema: Option[List[Schema]],
                     primaryKeys: Option[List[String]],
                     processIncremental:Option[Boolean]
                    )

  case class DataCart(
                       @com.fasterxml.jackson.databind.annotation.JsonDeserialize(contentAs=classOf[java.lang.Long])
                       jdbcDetail:SourceJDBC,
                       fileDetail:SourceFile,
                       commonDetail:CommonForAll,
                       target:Target
                     ) {

    def getBaseTargetTableName(): String = {
      target.tableName + "__base"
    }
    def getTempTargetTableName(): String = {
      target.tableName + "__tmp"
    }
    def getIncrementalTargetTableName(): String = {
      target.tableName + "__incremental"
    }
    def getTargetSchemaTable(): String = {
      val targetDataBase = target.dataBase
      if (targetDataBase.isDefined) s"${targetDataBase.get}.${target.tableName}" else target.tableName
    }
    def getBaseSchemaTable(): String = {
      val targetDataBase = target.dataBase
      if (targetDataBase.isDefined) s"${targetDataBase.get}.${getBaseTargetTableName()}" else getBaseTargetTableName()
    }
    def getTempSchemaTable(): String = {
      val targetDataBase = target.dataBase
      if (targetDataBase.isDefined) s"${targetDataBase.get}.${getTempTargetTableName()}" else getTempTargetTableName()
    }
  }


}
