# Spark Slope

#build
sbt package

#run it
spark-submit  --class "RegSort"   --master local[5] ./target/scala-2.11/regsort_2.11-1.0.jar

#run in debug modle
spark-submit  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 --class "RegSort" --jars ./lib/elasticsearch-spark-20_2.11-6.3.2.jar --master local[5] ./target/scala-2.11/regsort_2.11-1.0.jar


