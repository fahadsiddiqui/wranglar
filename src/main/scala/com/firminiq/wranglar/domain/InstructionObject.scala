package com.firminiq.wranglar.domain


object InstructionObject {

  case class FileDescriptor(delimiter:Option[String],isHeaderInclude:Option[String],compression: Option[String],format:Option[String],location:String, escape :Option[String], quote:Option[String], schemaObject:Option[List[SchemaObject]])
  case class UserInformation(key:String,secret:String)
  case class BusinessKey(businessKey:String , orderByKey:String, orderByType:Option[String])
  case class SelectClause(columnName: String, alias: Option[String])
  case class JoinColumn(leftSource: String, leftColumn: String, rightSource: String, rightColumn: String, compareType: Option[String], operator: Option[String])
  case class JoinDriver(tableName: String, tableAlias: String, selectClause: List[SelectClause])
  case class JoinObject(tableName: String, tableAlias: String, selectClause: List[SelectClause], joinType: String, condition: List[JoinColumn], conditionString:Option[String])
  case class SplitCondition(condition: String, selectClause: String, targetTable: String)
  case class UnionSource(tableName:String, condition:Option[String], selectClause:Option[String])
  case class HiveInstruction(databaseName:String, tableName:String,isManaged: Option[Boolean],selectClause:Option[String])
  case class PartitionInstruction(partitionColumn:List[String],numOfPartition: Option[Int])
  case class SchemaObject(columnName:String, dataType:String)
  case class UserAzureCredential(azureTokenRefreshUrl:String, azureClientId:String, azureClientCredential:String)

  case class CaseCondition(condition:String , value:Any, isConstant:Option[Boolean], dataType:Option[String])
  case class CaseInstruction(alias:String , conditions:List[CaseCondition])

  case class LoadS3File(operationType:String, label:Option[String],fileDescriptor: Option[FileDescriptor], userInfo:Option[UserInformation])
  case class LoadHDFSFile(operationType:String, label:Option[String],fileDescriptor: Option[FileDescriptor])
  case class LoadADLFile(operationType:String, label:Option[String],fileDescriptor: Option[FileDescriptor], userInfo:Option[UserAzureCredential])

  case class LoadHiveTable(operationType:String, hiveInstruction:HiveInstruction, label:Option[String])
  case class Filter(operationType:String, selectClause:Option[List[String]],sourceTable:String,condition:String, label:Option[String])
  case class Group(operationType:String,groupByExpression:String, aggregateExpression:String, orderByExpression:Option[String], sourceTable:String,label:Option[String])
  case class Deduplicate(operationType:String,isDistinct:Option[Boolean],selectClause:Option[List[String]], businessKey: Option[BusinessKey],sourceTable:String,label:Option[String])
  case class Pivot(operationType:String, pivotColumn: String, selectClause:Option[List[String]],aggregateFunction: String, aggregateColumn: String,sourceTable:String,label:Option[String])
  case class UserDefinedSql(operationType:String, sql:String, label:Option[String])
  case class Join(operationType:String, driverTable:JoinDriver,otherObjects:List[JoinObject] ,label:Option[String])
  case class Split(operationType:String,sourceTable:String,conditions:List[SplitCondition])
  case class Union(operationType:String,sources:List[UnionSource], unionType:Option[String], label:Option[String])
  case class Case(operationType: String,sourceTable:String, selectClause:Option[List[String]], caseInstruction: List[CaseInstruction], label: Option[String])

  case class Save(operationType:String,sourceTable:String,selectClause:Option[String],writeMode: Option[String],
                  fileDescriptor: Option[FileDescriptor],hiveInstruction: Option[HiveInstruction],
                  partitionInstruction: Option[PartitionInstruction] , isCreateHiveTable:Option[Boolean],
                  storageSystem:String, properties: Map[String,String]= Map())

}
