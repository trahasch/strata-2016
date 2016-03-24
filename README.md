Installation Requirements:
--------------------------
1.	Scala IDE - Either IntelliJ   OR   ScalaIDE for Eclipse are needed
	* IntelliJ
		* Download from https://www.jetbrains.com/idea/download/
		* Add Scala Plugin
		* Add Scala SDK : 2.10.4
		* https://www.jetbrains.com/help/idea/2016.1/creating-and-running-your-scala-application.html 
	* ScalaIDE for Eclipse
		* Download from http://scala-ide.org/download/sdk.html 
2. 	Apache Zeppelin

   	* Download source from https://zeppelin.incubator.apache.org/download.html
   	
   	* Compile zeppelin
    	```
    	mvn clean package -DskipTests -Pspark-1.6 -Phadoop-2.6 -Ppyspark
   		```
   	* Configuration files are at (usually not needed)
   	
		```
		./conf/zeppelin-env.sh
		
		./conf/zeppelin-site.xml
		```
		
	* Run the Zeppelin daemon
	
		```
		The command for managing the zeppelin process is
			./bin/zeppelin-daemon.sh start|stop|status|restart
		
		So if you have compiled Zeppelin in ~/Downloads/zeppelin-0.5.6-incubating, then
		to start you would use the command
		
		~/Downloads/zeppelin-0.5.6-incubating/bin/zeppelin-daemon.sh start
		```
		
	* cd to the directory where you have downloaded the Tutorial data
	
		```
		cd /Volumes/sdxc-01/Strata-2016/
		```
		
	* Run Zeppelin IDE in browser
	
		```
		localhost:8080
		```
		
3. Maven
	* https://maven.apache.org/install.html 
4. Git 
5. Download or git clone the tutorial code & data from https://github.com/jayantshekhar/strata-2016 (You are here!!)
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





