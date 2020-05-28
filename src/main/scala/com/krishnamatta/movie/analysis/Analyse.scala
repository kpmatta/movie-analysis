package com.krishnamatta.movie.analysis

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Analyse {

  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  lazy val projectDir: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {

    val inputMovieGenresPath = projectDir + "/InputFiles/ml-1m/movies.dat"
    val outputMovieGenresPath = projectDir + "/target/MovieGenres/"
    val movieDF = loadMovieData(inputMovieGenresPath)
    val movieGenresDF = getNumberOfMoviesPerGenres(movieDF)
    writeOutput(movieGenresDF.coalesce(1), outputMovieGenresPath, "csv")

    val inputMovieRatingsPath = projectDir + "/InputFiles/ml-1m/ratings.dat"
    val outputMovieRatingsPath = projectDir + "/target/TopMovies/"
    val ratingsDF = loadRatingsData(inputMovieRatingsPath)
    val topMoviesDF = getTopRatedMovie(ratingsDF, movieDF, 100)
    writeOutput(topMoviesDF, outputMovieRatingsPath, "parquet")
  }

  /**
    *
    * @param moviesFilePath : input file path
    * @return               : DataFrame
    */
  def loadMovieData(moviesFilePath : String) : DataFrame = {
    import spark.implicits._
    spark.read.text(moviesFilePath)
      .map(_.getString(0).split("::"))
      .map(row => Movies(row(0).toLong, row(1), row(2)))
      .toDF

    // Alternative way of reading with schema file
//    val schema = StructType(List(
//      StructField("MovieID", LongType, false),
//      StructField("Title", StringType, false),
//      StructField("Genres", StringType, false)
//    ))
//    ReadFile.loadCsv(moviesFilePath, "::", schema)
  }

  /**
    *
    * @param ratingsFilePath   : input file path
    * @return                  : DataFrame
    */
  def loadRatingsData(ratingsFilePath : String): DataFrame = {
    import spark.implicits._
    spark.sparkContext.textFile(ratingsFilePath)
      .map(_.split("::"))
      .map(row => Ratings(row(0).toLong, row(1).toLong, row(2).toInt, row(3).toLong))
      .toDF()

    // Alternative way of reading with schema file
//    val schema = StructType(List(
//      StructField("UserID", LongType, false),
//      StructField("MovieID", LongType, false),
//      StructField("Rating", IntegerType, false),
//      StructField("Timestamp", LongType, false)
//    ))
//    ReadFile.loadCsv(ratingsFilePath, "::", schema)
  }

  // A. CSV file containing list of unique Genres and no of movies under each genres
  // CSV file should contain 2 columns, ie: Genres, No of Movies. Column headers are not required.
  def getNumberOfMoviesPerGenres(movieDF : DataFrame): DataFrame = {

    // split Genres column and explode to expand rows.
    movieDF
      .withColumn("Genres", explode(split(col("Genres"), "\\|")))
      .groupBy("Genres")
      .count()
      .orderBy("Genres")
  }

  // B. Generate a parquet file that contain the top 100 movies based on their ratings.
  // This should have fields, Rank (1-100), Movie Id, Title, Average Rating. Rank 1 is the most popular movie.

  /**
    *
    * @param ratingsDF  : Input ratings dataframe
    * @param movieDF    : input movie dataframe
    */
  def getTopRatedMovie(ratingsDF : DataFrame,
                       movieDF : DataFrame,
                       top : Int ) : DataFrame = {

    val winSpec = Window.orderBy(desc("Average_Rating"))

    ratingsDF
      .groupBy("MovieID")
      .agg(avg("Rating").alias("Average_Rating"))
      .withColumn("Rank", rank().over(winSpec))
      .join(movieDF.as("movies"), movieDF("MovieID") === ratingsDF("MovieID"))
      .select("Rank", "movies.MovieID", "movies.Title", "Average_Rating")
      .orderBy("Rank")
      .limit(top)
  }

  /**
    *
    * @param outDF      : DataFrame to write
    * @param outputPath : output path to write
    * @param format     : format to write
    */
  def writeOutput(outDF : DataFrame,
                  outputPath : String,
                  format: String ) : Unit = {
    outDF.write
        .option("header", false)
        .format(format)
        .mode(SaveMode.Overwrite)
        .save(outputPath)
  }

}
