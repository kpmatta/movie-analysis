package com.krishnamatta.movie.analysis

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ReadFile {
  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
    *
    * @param filePath : file path to load
    * @param schema   : schema to apply on the data
    * @return         : DataFrame
    */
  def loadCsv(filePath : String,
              delimiter : String,
              schema: StructType): DataFrame = {
    val movieRDD = spark.sparkContext.textFile(filePath)
      .map(line => convertRow(line, delimiter, schema))
      .filter(_.isDefined)
      .map(_.get)

    spark.createDataFrame(movieRDD, schema)
  }

  /**
    *
    * @param line       : input line
    * @param delimiter  : delimiter to parse the data
    * @param schema     : schema to apply
    * @return           : updated row or none on exception
    */
  def convertRow(line: String, delimiter: String, schema: StructType): Option[Row] = {
    try {
      val res = (line.split(delimiter), schema)
        .zipped
        .map { case (fVal, field) => convertField(fVal, field.dataType) }

      Option(Row(res: _*))
    }
    catch {
      case ex: Exception =>
        println(ex.toString)
        None
    }
  }

  // TODO: Not all types are implemented.
  // TODO: Null types are not handled.
  /**
    *
    * @param fldVal   : input fields to convert
    * @param fldType  : destination type
    * @return         : converted value
    */
  def convertField(fldVal: String, fldType: DataType): Any = {
    fldType match {
      case IntegerType => fldVal.toDouble.toInt
      case LongType => fldVal.toDouble.toLong
      case FloatType => fldVal.toDouble.toFloat
      case DoubleType => fldVal.toDouble
      case _ => fldVal
    }
  }
}
