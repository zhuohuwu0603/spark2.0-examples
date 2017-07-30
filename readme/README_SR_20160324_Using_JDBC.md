
# using-jdbc

This project support spark 2.1.1 and 4 language in one repo: scala, java, python, r 

and use command to submit and run 4 languages. 

# Blog: 
https://sparkour.urizone.net/recipes/using-jdbc/

https://ddmsence.atlassian.net/projects/SPARKOUR/issues/SPARKOUR-18?filter=allissues

# run 

- download jdbc client:
  
  wget http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.38/mysql-connector-java-5.1.38.jar
  wget http://central.maven.org/maven2/postgresql/postgresql/9.1-901-1.jdbc4/postgresql-9.1-901-1.jdbc4.jar
  wget http://central.maven.org/maven2/org/xerial/sqlite-jdbc/3.8.11.2/sqlite-jdbc-3.8.11.2.jar
  
  Oracle now requires you to login to their portal to download the thin client JAR file.

- local[4] 
in sparkour.sh: change the SPARKOUR_HOME
    
SPARKOUR_HOME="/Users/kevinwu/Documents/zw_codes/GitLab/spark2_codes/"

- Scala
    Run in Local Mode
    ./sparkour.sh scala --driver-class-path lib/mysql-connector-java-5.1.38.jar
     
    Run against a Spark cluster
    
    // download mysql-connector-java-5.1.38.jar, and edit your $SPARK_HOME/conf/spark-defaults.conf
    spark.driver.extraClassPath /someDirectoryOnClusterNode/mysql-connector-java-5.1.38.jar
    spark.executor.extraClassPath /someDirectoryOnClusterNode/mysql-connector-java-5.1.38.jar    
    
    ./sparkour.sh scala --master spark://ip-172-31-24-101:7077
    
Or run in intellij, add an external jar in classpath:
```
https://stackoverflow.com/questions/1051640/correct-way-to-add-external-jars-lib-jar-to-an-intellij-idea-project/32853178

- Click File from File menu
- Project Structure (CTRL + SHIFT + ALT + S on Windows/Linux, ⌘ + ; on Mac OS X)
- Select Modules at the left panel
- Dependencies tab
- '+' → JARs or directories
```    

Or try (not tested yet): 
        
        -Dspark.master=local[4] -classpath lib/mysql-connector-java-5.1.38.jar
     
- Java: 
     ./sparkour.sh java --driver-class-path lib/mysql-connector-java-5.1.38.jar

- python: 
     ./sparkour.sh python --driver-class-path lib/mysql-connector-java-5.1.38.jar 
     
- r : 

Need first check if the updated_people table exist or not, if exist, delete it first. 
    show databases;
    use sparkour;
    show tables;
    select * from sparkour.updated_people;
    drop table sparkour.updated_people;

     ./sparkour.sh r --driver-class-path lib/mysql-connector-java-5.1.38.jar    