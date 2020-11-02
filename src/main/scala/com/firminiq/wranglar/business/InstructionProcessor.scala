package com.firminiq.wranglar.business

import com.firminiq.wranglar.domain.InstructionObject._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.json4s._
import org.slf4j.LoggerFactory

object InstructionProcessor {
  def apply() = new InstructionProcessor()
}

class InstructionProcessor {
  val log = LoggerFactory.getLogger(this.getClass)

  def readFromHive(spark:SparkSession,instruction: LoadHiveTable):DataFrame = {
    val  fromTableName  = instruction.hiveInstruction.tableName
    val  databaseName = instruction.hiveInstruction.databaseName
    val  tableName = instruction.label.getOrElse("")
    val sql =
      s"""SELECT ${instruction.hiveInstruction.selectClause.getOrElse("*")}
         |FROM $databaseName.$tableName
         |""".stripMargin
   val result =  spark.sql(sql)
    if (!(tableName.equalsIgnoreCase("")))result.createOrReplaceTempView(tableName)
    result
  }

  def readFromS3(spark:SparkSession, instruction: LoadS3File) :DataFrame  = {
    import com.firminiq.wranglar.util.Config._

    val userInfo = instruction.userInfo.getOrElse(new UserInformation("",""))
    val fileInfo = instruction.fileDescriptor.get
    val tableName = instruction.label.getOrElse("")
    val c  = Map(S3_KEY -> userInfo.key, S3_SECRET->userInfo.secret )
    log.info(s"s3token => $c")
    setupSparkContextForS3(spark, c)
    //val format = getFormatRead(fileInfo.format.getOrElse("parquet"))
    //TODO   create method  to  read different formats
    val  result = readdFiles(spark, fileInfo)

    if (!(tableName.equalsIgnoreCase("")))result.createOrReplaceTempView(tableName)
    result
  }

  def  readFromADLS(spark:SparkSession , instruction: LoadADLFile):DataFrame = {

    val userInfo = instruction.userInfo.get
    val fileInfo = instruction.fileDescriptor.get
    val tableName = instruction.label.getOrElse("")

    val azureTokenProviderType = userInfo.azureClientId
    val azureTokenRefreshUrl = userInfo.azureTokenRefreshUrl
    val azureClientId = userInfo.azureClientId
    val azureClientCredential = userInfo.azureClientCredential

     val prop = Map("azureTokenProviderType" -> azureTokenProviderType,"azureTokenRefreshUrl"->azureTokenRefreshUrl,"azureClientId"-> azureClientId,"azureClientCredential"-> azureClientCredential)

    setUpSparkContexctForADLS(spark,prop)

    val  result = readdFiles(spark, fileInfo)

    if (!(tableName.equalsIgnoreCase("")))result.createOrReplaceTempView(tableName)
    result
  }

  def readFromHDFS(spark:SparkSession, instruction: LoadHDFSFile) :DataFrame  = {

    val fileInfo = instruction.fileDescriptor.get
    val tableName = instruction.label.getOrElse("")
    val  result = readdFiles(spark, fileInfo)

    if (!(tableName.equalsIgnoreCase("")))result.createOrReplaceTempView(tableName)
    result
  }


  /**
    *
    * @param df
    * @param instruction
    * @return filtered DF
    */
 def filter(dataFrameMap:Map[String,DataFrame], instruction:Filter):DataFrame = {
   val  filterCondition = instruction.condition
   val sourceTable = instruction.sourceTable
   val selectColumns = instruction.selectClause.getOrElse(List())
   val tableName = instruction.label.getOrElse("")
   val df = dataFrameMap(sourceTable)
   val filteredDf = df.filter(filterCondition)

   log.debug(s"Filtered Condition $filterCondition")
   log.debug(s"Columns to Select  for Filter ${selectColumns.mkString(",")}")
   log.debug(s"Filter out put table name $tableName")

   val result  = if (selectColumns.isEmpty) filteredDf else filteredDf.select(selectColumns.head, selectColumns.tail: _*)
   if (!(tableName.equalsIgnoreCase("")))result.createOrReplaceTempView(tableName)
   result
 }

  /**
    *
    * @param df
    * @param instruction
    * @return
    */

  def group(spark:SparkSession ,instruction: Group):DataFrame = {
  val groupColumn = instruction.groupByExpression
  val aggregateExpression = instruction.aggregateExpression
  val orderBy = instruction.orderByExpression.getOrElse("Manju")
  val orderByExpression = if (orderBy.equalsIgnoreCase("manju")) "" else s"ORDER BY $orderBy "

  val sourceTable = instruction.sourceTable

  val sql =
    s""" SELECT $groupColumn  , $aggregateExpression
       | FROM $sourceTable
       | GROUP BY $groupColumn $orderByExpression
     """.stripMargin

 log.info(s" SQL going to execujted at group Method si $sql")

  val result = spark.sql(sql)

  val tableName = instruction.label.getOrElse("")
    if (tableName.nonEmpty) result.createOrReplaceTempView(viewName = tableName)

    result
}

  def deDup(dataFrameMap:Map[String,DataFrame], instruction:Deduplicate):DataFrame = {
    val columns = instruction.selectClause.getOrElse(List("*"))
    val sourceTable = instruction.sourceTable
    val tableName = instruction.label.getOrElse("")

    val df = dataFrameMap.get(sourceTable).get

    val spark = df.sparkSession

    val isDistinct = instruction.isDistinct.getOrElse(true)
    df.createOrReplaceTempView(viewName = sourceTable)
    val result = if (isDistinct) getDistinct(df, columns) else removeDuplicates(sourceTable,spark,instruction.businessKey.get,columns)
    if (tableName.nonEmpty) result.createOrReplaceTempView(viewName = tableName)
    result
  }

  def pivot(dataFrameMap:Map[String,DataFrame], instruction: Pivot):DataFrame = {
    val columns = instruction.selectClause.getOrElse(List())
    val pivotColumn = instruction.pivotColumn
    val aggregateFunction = instruction.aggregateFunction
    val aggregateColumn = instruction.aggregateColumn

    val df = dataFrameMap.get(instruction.sourceTable).get

    val dfCleanedPivotColumn = df.withColumn(pivotColumn, regexp_replace(df(pivotColumn), "[^a-zA-Z0-9_]", "_"))

    val pivotedDf =  if (columns.nonEmpty) dfCleanedPivotColumn.groupBy(columns.head, columns.tail: _*).pivot(pivotColumn) else
                         dfCleanedPivotColumn.groupBy(columns.head).pivot(pivotColumn)
    val result = getPivotedDf(pivotedDf,aggregateFunction,aggregateColumn)

    val tableName = instruction.label.getOrElse("")

    if (tableName.nonEmpty) result.createOrReplaceTempView(viewName = tableName)

    result
  }

  def userDefinedSql(spark:SparkSession, instruction: UserDefinedSql):DataFrame = {
    val result = spark.sql(instruction.sql)

    val tableName = instruction.label.getOrElse("")

    if (tableName.nonEmpty) result.createOrReplaceTempView(viewName = tableName)
    result
  }

  def join (spark:SparkSession,instruction: Join) :DataFrame = {
    val diverInstruction = instruction.driverTable
    val otherInstructions = instruction.otherObjects
    val driverSelectClause = diverInstruction.selectClause
    val driverSelectFinal = "SELECT " + {if (driverSelectClause.isEmpty) "" else  getSelectCause(diverInstruction.tableAlias, driverSelectClause)}
    val driverFromFinal = s"  FROM  ${diverInstruction.tableName}  ${diverInstruction.tableAlias} "

    val driver = JoinWrapper(driverSelectFinal,driverFromFinal)
    val sql = prepareJoinSql(driver,otherInstructions)
    log.info(s"SQl generated by join $sql")
   val result =  spark.sql(sql)

    val tableName = instruction.label.getOrElse("")
    if (tableName.nonEmpty) result.createOrReplaceTempView(viewName = tableName)

    result
  }

  def union(spark:SparkSession,instruction: Union):DataFrame = {
    val dfList  = instruction.sources.map(x =>getDfFromUnionInstruction(x.tableName,x.selectClause.getOrElse("*"),x.condition,spark))
   val unionDf =  dfList.reduce(_ union  _)

    val unionType = instruction.unionType.getOrElse("unionAll")
    val result = if (unionType.equalsIgnoreCase("unionAll")) unionDf else unionDf.distinct()

    val tableName = instruction.label.getOrElse("")
    if (tableName.nonEmpty) result.createOrReplaceTempView(viewName = tableName)

    result.repartition()

    result
  }

  def split(dataFrameMap:Map[String,DataFrame], instruction:Split):List[ResultWrapper] = {
    val sourceTable = instruction.sourceTable
    val df = dataFrameMap.get(sourceTable).get

    df.explain()

    instruction.conditions.map(x => getOneSplitDf(df,x))
  }


  def Case(dataFrameMap:Map[String,DataFrame], instruction:Case):DataFrame = {
    val selectClause = instruction.selectClause.getOrElse(List("*")).mkString(",")
    val tableName = instruction.label.getOrElse("")
    val sourceTable  = instruction.sourceTable
    val spark = dataFrameMap.get(sourceTable).get.sparkSession
    val caseInstructions = instruction.caseInstruction

    val caseSql = if (caseInstructions.nonEmpty) getCaseStatements(caseInstructions) else ""

    val sql = if (caseSql.equalsIgnoreCase("") || caseSql.isEmpty) s"select $selectClause  FROM $sourceTable" else s"select $selectClause , $caseSql  FROM $sourceTable"

    log.info(s"Case Statement SQl>>> $sql")

    val result = spark.sql(sql)
    
    if (tableName.nonEmpty) result.createOrReplaceTempView(viewName = tableName)

    result
  }
  

  def save(dataFrameMap:Map[String,DataFrame], instruction:Save):Unit = {
    val sourceTable = instruction.sourceTable

    val df = dataFrameMap.get(sourceTable).get


    val spark = df.sparkSession
    val storageSystem = instruction.storageSystem

    storageSystem.toLowerCase() match {
      case "s3" => setupSparkContextForS3(spark,instruction.properties )
      case "ADLS" => setUpSparkContexctForADLS(spark,instruction.properties )
        //TODO add to GC
      case _ => spark
    }

    val hiveInstruction = instruction.hiveInstruction.get
    val fileDesc = instruction.fileDescriptor.get
    val parDesc  = instruction.partitionInstruction.get
    val partitionColumns  = parDesc.partitionColumn
    val location  = fileDesc.location



    if (partitionColumns.nonEmpty) {
      saveDfWithPartition(df,partitionColumns,location,instruction.writeMode,parDesc.numOfPartition,fileDesc.format,
        fileDesc.compression,fileDesc.delimiter.getOrElse("|"))
    } else {
      saveDfWithNoPartition(df,location,instruction.writeMode,parDesc.numOfPartition,fileDesc.format,
        fileDesc.compression,fileDesc.delimiter.getOrElse("|"))
    }

    if (instruction.isCreateHiveTable.getOrElse(false)) {
      createTable(df,hiveInstruction.tableName,Some(hiveInstruction.databaseName),location,
        fileDesc.format.getOrElse("parquet"), partitionColumns,spark,fileDesc.delimiter.getOrElse("|"),Map()
      )
    }
  }



//Method called from Job
  /**
    *
    * @param instruction
    * @param spark
    */
  def init(instruction:JsonAST.JValue, spark:SparkSession, tableDataFrameMap:scala.collection.mutable.Map[String, DataFrame] ):Unit = {
    implicit val formats = DefaultFormats

    (instruction \ "operationType") match {
      case JString("HIVE") => {
        //TODO change it Hive
        val obj = instruction.extract[LoadHiveTable]
        val df = readFromHive(spark,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("S3") => {
        //TODO change it S3
        val obj = instruction.extract[LoadS3File]
        val df = readFromS3(spark,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("ADLS") => {
        //TODO change it ADLS
        val obj = instruction.extract[LoadADLFile]
        val df = readFromADLS(spark,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("GC") => {
        //TODO change it GC
        //val obj = instruction.extract[LoadADLFile]
        //val df = readFromADLS(spark,obj)
        //tableDataFramemap.put(obj.label.get, df)
      }
      case JString("HDFS") => {
        //TODO change it HDFS
        val obj = instruction.extract[LoadHDFSFile]
        val df = readFromHDFS(spark,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }

      case JString("FILTER") => {
        val obj = instruction.extract[Filter]
        val df = filter(tableDataFrameMap.toMap,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("GROUP") => {
        val obj = instruction.extract[Group]
        val df = group(spark,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("DEDUP") => {
        val obj = instruction.extract[Deduplicate]
        val df = deDup(tableDataFrameMap.toMap,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("PIVOT") => {
        val obj = instruction.extract[Pivot]
        val df = pivot(tableDataFrameMap.toMap,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("UDS") => {
        val obj = instruction.extract[UserDefinedSql]
        val df = userDefinedSql(spark,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("JOIN") => {
        val obj = instruction.extract[Join]
        val df = join(spark,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("UNION") => {
        val obj = instruction.extract[Union]
        val df = union(spark,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }
      case JString("CASE") => {
        val obj = instruction.extract[Case]
        val df = Case(tableDataFrameMap.toMap,obj)
        tableDataFrameMap.put(obj.label.get, df)
      }

      case JString("SPLIT") => {
        val obj = instruction.extract[Split]
        val results = split(tableDataFrameMap.toMap,obj)
        results.map(x => tableDataFrameMap.put(x.tableName,x.df))
      }

      case JString("SAVE") => {
        val obj = instruction.extract[Save]
        val df = save(tableDataFrameMap.toMap,obj)
      }
    }
  }


private def getOneSplitDf(df:DataFrame,split:SplitCondition):ResultWrapper = {
  val tableName = split.targetTable
  val condition = split.condition
  val selectClause = split.selectClause.split(",")
  val result = df.filter(condition)
    .select(selectClause.head, selectClause.tail: _*)

  result.createOrReplaceGlobalTempView(tableName)

  ResultWrapper(result, tableName)
}


  private def saveDfWithPartition(df: DataFrame, partitionColumn: List[String], location: String,
                                  writeMode: Option[String], numOfPartition: Option[Int],
                                  fileFormat: Option[String], compression: Option[String] = None,
                                  delimiter:String="|"):Unit = {

    val derivedPartitionSize = getPartitionSize(df)
    val partitionSize = numOfPartition.getOrElse(derivedPartitionSize)
    val derivedFileFormat = fileFormat.getOrElse("parquet")
    val wm = writeMode.getOrElse("overwrite")

    log.info(s"""saveDfWithPartition partitionSize $partitionSize, partitionColumns ${partitionColumn.mkString(",")},
                  fileFormat $derivedFileFormat, writeMode $wm,
                  compression $compression, location $location""")

    df.repartition(partitionSize)
      .write
      .format(getFormat(derivedFileFormat))
      .mode(getWriteMode(wm))
      .partitionBy(partitionColumn.toArray: _*)
      .option("compression", compression.getOrElse("snappy"))
      .save(location)
  }

  private def saveDfWithNoPartition(df: DataFrame, location: String, writeMode: Option[String], numOfPartition: Option[Int],
                                    fileFormat: Option[String], compression: Option[String] = None, delimiters:String="|",
                                    options: Map[String, String] = Map() ):Unit = {
    val derivedPartitionSize = getPartitionSize(df)

    val partitionSize = numOfPartition.getOrElse(derivedPartitionSize)
    val derivedFileFormat = fileFormat.getOrElse("parquet")
    val wm = writeMode.getOrElse("overwrite")
    val isRepartition  = options.get("repartition").orElse(Option("true")) == Option("true")

    log.info(s"saveDfWithNoPartition partitionSize $partitionSize, fileFormat $derivedFileFormat, writeMode $wm " +
      s"isRepartition $isRepartition, location $location")

    val resizedDf = if (isRepartition) df.repartition(partitionSize) else df
    val delimiter = options.getOrElse("delimiter", delimiters)

    var extraOptions:Map[String, String] = derivedFileFormat match {
      case "csv" | "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat" => {
        //"timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSS", "dateFormat" -> "yyyy-MM-dd"
        val tsMap: Map[String, String] = if (options.get("timestampFormat").isDefined) Map("timestampFormat" -> options.get("timestampFormat").get) else Map[String, String]()
        Map("delimiter" -> delimiter) ++ tsMap
      }
      case _   => Map[String, String]()
    }
    resizedDf
      .write
      .format(getFormat(derivedFileFormat))
      .mode(getWriteMode(wm))
      .option("compression", compression.getOrElse("snappy"))
      .options(extraOptions)
      .save(location)
  }

  def createTable (df: DataFrame, tableName:String, dataBaseName:Option[String], location: String,fileFormat:String,
                   partitionColumn: List[String],spark:SparkSession,delimiter:String=",", options: Map[String, String] = Map() ) = {


    val isManaged = options.get("isManagedTable").orElse(Option("false")) == Option("true")

    val shortFormat = getFormatShort(fileFormat)


    if (shortFormat == "csv" || shortFormat == "avro") {
      val  tableCreateWrapperObj = getCreateTableSql(df, partitionColumn, location, tableName,
        dataBaseName, Option(isManaged), shortFormat, delimiter, options)
      log.info(s"exec =>  ${tableCreateWrapperObj.sql}" )
      spark.sql(tableCreateWrapperObj.sql)
    } else {
      val tableCreateWrapperObj = getCreateTableSql(df, partitionColumn, location, tableName, dataBaseName, Option(isManaged), fileFormat, null)
      log.info(s"exec =>  ${tableCreateWrapperObj.sql}" )
      spark.sql(tableCreateWrapperObj.sql)
    }
  }

  def getCreateTableSql(df: DataFrame, partitionColumn: List[String], location: String, tableName: String, dataBase: Option[String] = Some("amp"),
                        isManaged: Option[Boolean] = Some(false), fileFormat: String = "parquet", separator: String = "|",
                        options: Map[String, String] = Map()): TableCreateWrapper = {

    log.info(s"separator $separator, options => $options")

    lazy  val schema = if (options.get("convertDateToString") == Some("true")) {
      df.schema.fields.map {
        x => {
          val t = if (x.dataType.typeName == "date" || x.dataType.typeName == "timestamp") "string" else x.dataType.typeName
          (Column(x.name, t))
        }
      }.toList
    } else {
      df.schema.fields.map(x => (Column(x.name, x.dataType.typeName))).toList
    }

    var updatedSeparator  = separator
    if (separator == "\u0001") {
      updatedSeparator = "\\001"
    }


    //lazy val schema = df.schema.fields.map(x => (x.name, x.dataType.typeName)).toList
    lazy val databaseName = dataBase.getOrElse("default") + "."
    lazy val tblName= databaseName + tableName
    lazy val tableSql = if (isManaged.getOrElse(false)) s"CREATE EXTERNAL TABLE IF NOT EXISTS  $tblName" else s"CREATE TABLE  IF NOT EXISTS $tblName"
    lazy val nonPartitionSqlObj = getSqlForNonPartitionColumns(schema, partitionColumn)
    lazy val  partitionSqlObj = getSqlForPartitionColumns(schema, partitionColumn)
    lazy val storageHandle = if (fileFormat.equalsIgnoreCase("csv")) s" ROW FORMAT DELIMITED FIELDS TERMINATED BY '$updatedSeparator' STORED AS TEXTFILE " else s" STORED AS $fileFormat "
    lazy val createTableSqlPartition = s"""$tableSql
                                          |(${nonPartitionSqlObj.sql})
                                          |PARTITIONED BY (${partitionSqlObj.sql})
                                          |$storageHandle
                                          |LOCATION '$location' """.stripMargin
    lazy val createTableSqlNonPartition = s"""$tableSql
                                             |(${nonPartitionSqlObj.sql})
                                             |$storageHandle
                                             |LOCATION '$location' """.stripMargin
    val createTableSql = if (partitionColumn.nonEmpty) createTableSqlPartition else createTableSqlNonPartition
    log.info("Generated  create Table Sql  is " + createTableSql)

    TableCreateWrapper(createTableSql, nonPartitionSqlObj.columns, partitionSqlObj.columns)
  }


  private def getSqlForPartitionColumns(columns: List[Column], partitionColumn: List[String]): TableWrapper = {
    val colMap = columns.map(x=> (x.name,x.dataType)).toMap

    // Map will not work here  since it changes order
    val result = partitionColumn.map(x => Column(x,colMap.get(x).get))

    val sql = result.map(x => s"${x.name}  ${x.dataType}").mkString(" , ")
    val  columnsPart = result.map(x => x.name)

    TableWrapper(columnsPart,sql)
  }

  private def getSqlForNonPartitionColumns(columns: List[Column], partitionColumn: List[String]): TableWrapper = {

    val result = columns.filterNot(x => partitionColumn.contains(x.name))
    val sql = result.map(x => s"${x.name}  ${x.dataType}").mkString(" , ")
    val  columnsPart = result.map(x => x.name)

    TableWrapper(columnsPart,sql)
  }

  private def dropTable(spark:SparkSession, tableName: String) = {
    val sql = s"DROP TABLE IF EXISTS  $tableName PURGE"
    spark.sql(sql)
  }

  private def getPartitionSize(df: DataFrame): Int = {
    val coalesceSize = Math.min(Math.max(1, df.rdd.partitions.length/2), 200)
    coalesceSize
  }

 private  def performMsck(tablename:String,park: SparkSession ) = {
    try {
      park.sql(s"MSCK REPAIR TABLE  ${tablename}")
      log.info(s"Performing MSCK on table  $tablename")
    } catch {
      case _ => log.info(s" Could not Performing MSCK on table  $tablename")
    }
  }

 private  def getWriteMode(writeMode: String): String = {
    val diravedWriteMode =  writeMode.toLowerCase() match {
      case "append" => "append"
      case _ => "overwrite"
    }
   diravedWriteMode
  }

  private def getFormat(format: String): String = {
    val fmt = format.toLowerCase() match {
      case "parquet" => "parquet"
      case "csv" | "com.databricks.spark.csv" => "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"
      case "avro" => "com.databricks.spark.avro"
      case _ => format
    }
    fmt
  }

  private def getFormatShort(format: String): String = {
    val fmt = format.toLowerCase() match {
      case "parquet" => "parquet"
      case "avro" | "com.databricks.spark.avro" => "avro"
      case "csv" | "org.apache.spark.sql.execution.datasources.csv.csvfileformat" => "csv"
      case _ => format
    }
    fmt
  }


  private def getFormatRead(format: String): String = {
    val fmt = format.toLowerCase() match {
      case "parquet" => "parquet"
      case "avro" | "com.databricks.spark.avro" => "com.databricks.spark.avro"
      case "csv" | "org.apache.spark.sql.execution.datasources.csv.csvfileformat" => "csv"
      case _ => format
    }
    fmt
  }

//Pivot
  private def getPivotedDf(df:RelationalGroupedDataset, aggregateFunction:String, aggregateColumn:String):DataFrame = {
    aggregateFunction.toLowerCase() match {
      case "sum" => df.sum(aggregateColumn)
      case "avg" => df.avg(aggregateColumn)
      case "min" => df.min(aggregateColumn)
      case "max" => df.max(aggregateColumn)
      case "mean" => df.mean(aggregateColumn)
      case "count" => df.count()
      case _ => df.sum(aggregateColumn)
    }
  }

  //Supporting Methods

  /**
    *
    * @param driver
    * @param joinTables
    * @return
    */

  ///Join
  private def prepareJoinSql(driver:JoinWrapper,joinTables:List[JoinObject]):String = {
    val restOfJoin = joinTables.map(prepareSqlFromRestOfJoinTable)
    val joinSelect = restOfJoin.map(x => x.selectClause).filterNot(_.isEmpty).mkString(",")
    val joinCondition = restOfJoin.map(x => x.fromClause).mkString(" ")

    val fullSelect = if (joinSelect.isEmpty) driver.selectClause else s"${driver.selectClause}  , $joinSelect"
    val fullCondition = s"${driver.fromClause}  $joinCondition"

    log.debug(s"select column at prepareJoinSql : fullSelect " )
    log.debug(s"select Join  at prepareJoinSql :fullCondition " )
    s"$fullSelect $fullCondition"
  }

  private def prepareSqlFromRestOfJoinTable(joinObject:JoinObject):JoinWrapper = {
    val condition = joinObject.condition.map(prepareConditionStatement).mkString(" ").trim.replaceAll("\\S*$", "")
    val table = joinObject.tableName
    val tableAlias = joinObject.tableAlias
    val joinType = getJoinType(joinObject.joinType)
    val joinSelectClause = joinObject.selectClause
    val selectClause = if (joinSelectClause.nonEmpty) getSelectCause(tableAlias, joinObject.selectClause) else ""

    val fromClause = s"$joinType $table $tableAlias ON ($condition)"

    log.debug(s"select Clause Generated By  prepareSqlFromRestOfJoinTable $selectClause")
    log.debug(s"Join/From Clause Generated By  prepareSqlFromRestOfJoinTable $fromClause")

    JoinWrapper(selectClause,fromClause)
  }

  private def getSelectCause(tableAlias: String, selectColumns: List[SelectClause]): String = {
    selectColumns.map(x => tableAlias + "." + x.columnName + " AS " + x.alias.getOrElse(x.columnName)).mkString(",")
  }

  private def prepareConditionStatement(instruction: JoinColumn):String ={
    val op = instruction.operator.getOrElse("AND")
    val ct = instruction.compareType.getOrElse("=")
    val condition = s"${instruction.leftSource}.${instruction.leftColumn}  $ct  ${instruction.rightSource}.${instruction.rightColumn}  $op"
    log.debug(s"Condition Generated  by  prepareConditionStatement  $condition")
    condition
  }

  private def  getJoinType(joinType: String): String = {
    val typeOfJoin = joinType.toLowerCase() match {
      case "left" => "LEFT JOIN"
      case "right" => "RIGHT JOIN"
      case "full" => "FULL JOIN"
      case "join" => "JOIN"
      case "inner" => "JOIN"
      case _ => "JOIN"
    }
    typeOfJoin
  }

  //Join Ends

  //Union
  private  def getDfFromUnionInstruction(tableName:String, selectClause:String, condition:Option[String], spark:SparkSession):DataFrame = {
    val whereClause = condition.getOrElse("")
     val whereClauseFinal  = if (whereClause.equalsIgnoreCase("")) "" else s"WHERE $whereClause"
    val sql = s"SELECT $selectClause FROM $tableName $whereClauseFinal"
    log.debug(s"SQL generated  by getDfFromInstruction $sql")
    spark.sql(sql)
  }

  //dedup
  private def removeDuplicates(sourceTableName:String , spark:SparkSession, businessKey: BusinessKey, columns:List[String]):DataFrame = {
    val  sql =
      s""" SELECT * FROM (SELECT  ${columns.mkString(",")} ,
         | row_number() over (partition by ${businessKey.businessKey} order by ${businessKey.orderByKey} ${businessKey.orderByType.getOrElse("DESC")}) as idx
         | FROM $sourceTableName) rel WHERE idx = 1
     """.stripMargin

    log.info(s"Sql Generated  in removeDuplicates Method  is $sql ")
    spark.sql(sql).drop("idx")
  }

  private def getDistinct(df:DataFrame, columns:List[String]):DataFrame = {
    val result  = if (columns.head.equalsIgnoreCase("*")) df  else df.select(columns.head, columns.tail:_*)
    result.distinct()
  }

  //end of dedup


  private def getRandomName(salt:String): String = {
    val rndm = scala.util.Random
    salt + Math.abs(rndm.nextLong())
  }


  private def setupSparkContextForS3(spark: SparkSession, config: Map[String, String]): Unit = {
    import com.firminiq.wranglar.util.Config._

    val key = config(S3_KEY)
    val secret = config(S3_SECRET)


    if(key!=null && key!="" && secret!= null && secret!="" ){
      spark.conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      spark.conf.set(S3_KEY, key)
      spark.conf.set(S3_SECRET, secret)
    }

  }


  private def setUpSparkContexctForADLS(spark:SparkSession,properties:Map[String,String]):Unit = {
    spark.conf.set("dfs.adls.oauth2.access.token.provider.type", properties.get("azureTokenProviderType").get)
    spark.conf.set("dfs.adls.oauth2.client.id", properties.get("azureClientId").get)
    spark.conf.set("dfs.adls.oauth2.credential", properties.get("azureClientCredential").get)
    spark.conf.set("dfs.adls.oauth2.refresh.url", properties.get("azureTokenRefreshUrl").get)

    spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", spark.conf.get("dfs.adls.oauth2.access.token.provider.type"))
    spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", spark.conf.get("dfs.adls.oauth2.client.id"))
    spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", spark.conf.get("dfs.adls.oauth2.credential"))
    spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", spark.conf.get("dfs.adls.oauth2.refresh.url"))
  }

 private def readCSv(spark:SparkSession,fileDescriptor: FileDescriptor) :DataFrame = {
   val  delimiter = fileDescriptor.delimiter.getOrElse("|")
   val isHeader = fileDescriptor.isHeaderInclude.getOrElse("false")
   val escape  = fileDescriptor.escape.getOrElse("\\")
   val quote = fileDescriptor.quote.getOrElse("\"")

   val df = spark.read.format("csv")
          .option("sep", delimiter)
          .option("header", isHeader)
          .option("escape",escape)
          .option("quote",quote)
          .load(fileDescriptor.location)

  val dfFinal =  if (fileDescriptor.schemaObject.isDefined) {
    val schema = fileDescriptor.schemaObject.get
      val schemaObject = getSchema(schema)
     log.info(s"schema Object  $schemaObject")
     spark.createDataFrame(df.rdd,schemaObject)
    } else df
   dfFinal
 }

  private  def readdFiles(spark:SparkSession, fileDescriptor: FileDescriptor):DataFrame = {
    val locFormat = getFormatShort(fileDescriptor.format.getOrElse("parquet"))
    val loc = fileDescriptor.location
    locFormat.toLowerCase() match {
      case "parquet"  => spark.read.format("parquet").load(loc)
      case "avro" => spark.read.format("com.databricks.spark.avro").load(loc)
      case "csv" =>  readCSv(spark,fileDescriptor)
      case "json" => spark.read.json(loc)
      case _ => spark.read.format(locFormat).load(loc)
    }

  }

 private def getSchema(schemaList:List[SchemaObject]):StructType  = {

   schemaList.foldLeft(new org.apache.spark.sql.types.StructType())((sl,c) => {
     sl.add(c.columnName, c.columnName)
    }
   )
 }

  private def getCaseStatementOneCondition(instruction: CaseCondition): String = {
    lazy val condition = instruction.condition
    lazy val isConstant = instruction.isConstant.getOrElse(false)
    lazy val value = instruction.value
    lazy val dataType = instruction.dataType.getOrElse("")
    lazy val finalValue = if (isConstant) applyConstantValue(value, dataType) else value

    if (condition.equalsIgnoreCase("else")) s" ELSE $finalValue" else s"WHEN ($condition) THEN $finalValue"
  }

  private def applyConstantValue(value: Any, dataType: String): String = {
    dataType.toLowerCase() match {
      case "numeric" => s"$value"
      case "string" => s"'$value'"
      case _ => s"'$value'"
    }
  }

  private def getCaseStatementForAllCondition(instruction: CaseInstruction): String = {
    lazy val alias = instruction.alias
    val caseSql = instruction.conditions.map(getCaseStatementOneCondition).mkString(" ")

    val sql = s"CASE  $caseSql END AS $alias"
    log.info("Case statement  Generated >>>>>>" + sql)
    sql
  }

 private  def getCaseStatements(instruction: List[CaseInstruction]): String = {
    instruction.map(getCaseStatementForAllCondition).mkString(" , ")
  }

//Domain  class
  case class JoinWrapper(selectClause:String, fromClause:String)
  case class ResultWrapper(df:DataFrame, tableName:String)
  case class Column(name:String, dataType:String)
  case class TableWrapper(columns:List[String],sql:String)
  case class TableCreateWrapper(sql:String,columnsRegular:List[String],columnsPartioned:List[String])
}
