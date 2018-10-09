# Spark Slope

## Build it
sbt package

## Run it
    spark-submit  --class "RegSort"   --master local[5] ./target/scala-2.11/regsort_2.11-1.0.jar

## Run in debug mode
    spark-submit  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 --class "RegSort"  --master local[5] ./target/scala-2.11/regsort_2.11-1.0.jar


