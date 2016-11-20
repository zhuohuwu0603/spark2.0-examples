Examples for Spark 2.0 release.

# Build

    sbt clean package
    
    
# Add test
    - cmd + shift + T: create new test cases (it will auto-create the folder structure in test, if there is problem, run sbt clean and re-create)
    
# Add coverageReport
    - run the command: sbt clean coverage test coverageReport
    - report output can be seen: /Users/kevinwu/Documents/zw_codes/GitLab/spark2.0-examples/target/scala-2.11/scoverage-report/index.html 
     
# Add scalastyle check:
     - link: http://www.scalastyle.org/sbt.html
     - Add the following line to : project/plugins.sbt
        - addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")
     - Usage: sbt scalastyleGenerateConfig: create a scalastyle-config.xml in the current directory
        - check the scala style: sbt scalastyle
        
     - Spark code style guild: https://cwiki.apache.org/confluence/display/SPARK/Spark+Code+Style+Guide
     - Databricks Scala Guide: https://github.com/databricks/scala-style-guide