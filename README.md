Installation Requirements:
--------------------------
1.	Scala IDE - Eclipse or IntelliJ
2. 	Apache Zeppelin

   	* Download source from https://zeppelin.incubator.apache.org/download.html
   	
   	* Compile zeppelin
    	
    	mvn clean package -DskipTests -Pspark-1.6 -Phadoop-2.6 -Ppyspark
   	
   	* Configure (if needed)
		
		./conf/zeppelin-env.sh
		
		./conf/zeppelin-site.xml
	
	* Run the Zeppelin daemon
		
		./bin/zeppelin-daemon.sh start|stop|status|restart
		
		~/Downloads/zeppelin-0.5.6-incubating/bin/zeppelin-daemon.sh start
	
	* cd to the directory where you have downloaded the Tutorial data
		
		cd /Volumes/sdxc-01/Strata-2016/
	
	* Run IDE in browser
		
		localhost:8080
3. Maven
4. Git 
5. Download tutorial code and data from https://github.com/jayantshekhar/strata-2016
6. Apache Spark

Run KMeans
----------

spark-submit --class com.cloudera.spark.kmeans.JavaKMeans  --master yarn target/spark-mllib-1.0.jar data/kmeans 2 5

Run Movie Lens
--------------

spark-submit --class com.cloudera.spark.movie.JavaMovieLensALS  --master yarn target/spark-mllib-1.0.jar data/movielens_small 5 5

Run DataFrames version of Movie Lens
------------------------------------

spark-submit --class com.cloudera.spark.movie.JavaDFMovieLensALS  --master yarn target/spark-mllib-1.0-jar-with-dependencies.jar data/movielens/ratings 5 5

Run Iris
--------

spark-submit --class com.cloudera.spark.iris.JavaIris  --master yarn target/spark-mllib-1.0-jar-with-dependencies.jar data/iris 3 5

Run Covtype
-----------

spark-submit --class com.cloudera.spark.covtype.JavaCovtype  --master yarn target/spark-mllib-1.0-jar-with-dependencies.jar data/covtype


Run FPGrowth
------------

spark-submit --class com.cloudera.spark.fpg.JavaFPGrowth  --master yarn target/spark-mllib-1.0.jar data/fpg


Run Wikipedia PageRank
----------------------

spark-submit --class com.cloudera.spark.graphx.wikipedia.WikipediaPageRankFormat2  --master yarn target/spark-mllib-1.0.jar


Run Triangle Count
------------------

spark-submit --class com.cloudera.spark.graphx.triangle.TriangleCount  --master yarn target/spark-mllib-1.0.jar


Run PageRank
------------

spark-submit --class com.cloudera.spark.graphx.pagerank.PageRank  --master yarn target/spark-mllib-1.0.jar





