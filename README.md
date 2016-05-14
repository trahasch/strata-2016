Installation Requirements:
--------------------------
1.	ScalaIDE for Eclipse is needed

	* Download from http://scala-ide.org/download/sdk.html
	* For Projects imported into Eclipse set the Scala version to 2.10.6 under Project/Properties
2. Maven
	* https://maven.apache.org/install.html
3. 	Apache Zeppelin

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
		
	* cd to the directory where you have downloaded the Tutorial data
		```
		cd /Volumes/sdxc-01/Strata-2016/
		```
		
	* Run the Zeppelin daemon
		* The command for managing the zeppelin process is
			```./bin/zeppelin-daemon.sh start|stop|status|restart```
		* So if you have compiled Zeppelin in ~/Downloads/zeppelin-0.5.6-incubating, then
		to start you would use the command
			```~/Downloads/zeppelin-0.5.6-incubating/bin/zeppelin-daemon.sh start```	

	* Run Zeppelin IDE in browser
	
		```
		localhost:8080
		```	

4. Git (Nice to have)
	* https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
5. Tutorial code & data
	* Download or ```git clone``` the from https://github.com/jayantshekhar/strata-2016 (You are here!!)
6. Build with 'mvn package'
7. Import into IntelliJ/ScalaIDE for Eclipse as a maven project


