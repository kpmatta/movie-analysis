package com.krishnamatta.movie.analysis

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.apache.spark.sql.functions._

class AnalyseTest extends FunSuite with Matchers with BeforeAndAfter {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  lazy val projectDir : String = System.getProperty("user.dir")
  val inputMovieGenresPath  : String = projectDir + "/InputFiles/ml-1m/movies.dat"
  val inputMovieRatingsPath : String = projectDir + "/InputFiles/ml-1m/ratings.dat"

  test("testLoadMovieData") {
    val movieDF = Analyse.loadMovieData(inputMovieGenresPath)

    // number of columns 3
    movieDF.columns.length shouldBe 3

    // Successful read count
    movieDF.count() shouldBe 3883
  }

  test("testGetNumberOfMoviesPerGenres") {

    val expGenres = Array("Action", "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary","Drama",
      "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western")

    val movieDF = Analyse.loadMovieData(inputMovieGenresPath)
    val movieGenresDF = Analyse.getNumberOfMoviesPerGenres(movieDF)
    val resGenres = movieGenresDF.select("Genres").collect().map(_.getString(0))

    // compare unique genres with expected values
    resGenres shouldBe expGenres
  }

  test("testLoadRatingsData") {
    val movieRatingsDF = Analyse.loadRatingsData(inputMovieRatingsPath)

    // Validate data as per the readme documentation
    movieRatingsDF.columns.length shouldBe 4
    movieRatingsDF.agg(max("Rating")).head()getInt(0) shouldBe 5

    movieRatingsDF.agg(min("UserID")).head().getLong(0) shouldBe 1
    movieRatingsDF.agg(max("UserID")).head().getLong(0) shouldBe 6040

    movieRatingsDF.agg(min("MovieID")).head().getLong(0) shouldBe 1
    movieRatingsDF.agg(max("MovieID")).head().getLong(0) shouldBe 3952
  }

  test("testGetTopRatedMovie") {
    val top = 15  // load top 15 movies, assuming more than 15 movies.
    val movieRatingsDF = Analyse.loadRatingsData(inputMovieRatingsPath)
    val movieDF = Analyse.loadMovieData(inputMovieGenresPath)
    val topRatedMoviesDF = Analyse.getTopRatedMovie(movieRatingsDF, movieDF, top)

//    topRatedMoviesDF.explain()

    topRatedMoviesDF.count() shouldBe top
  }

  ignore("testFullTest") {
    Analyse.main(Array(""))

    val outputMovieGenresPath = projectDir + "/target/MovieGenres/"
    val outputMovieRatingsPath = projectDir + "/target/TopMovies/"

    spark.read.csv(outputMovieGenresPath).count() shouldBe 18
    spark.read.parquet(outputMovieRatingsPath).count()  shouldBe 100
  }
}
