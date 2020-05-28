# Movie Analysis 

### Build and test
mvn test
mvn package

### Run
- local spark submit

	%SPARK_HOME%/bin/spark-submit --class com.krishnamatta.movie.analysis.Analyse --master local[*] ./target/movie-analysis-1.0-SNAPSHOT.jar

### Test report
mvn surefire-report:report