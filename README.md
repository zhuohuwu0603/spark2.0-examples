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
     
# View sbt dependencies:
     - sbt "inspect tree clean" //no need to add the sbt-dependency-graph plug-in
     - sbt dependencyTree
     - sbt dependencyBrowseGraph

## More reference: 
- official github for sbt-dependency-graph:
https://github.com/jrudolph/sbt-dependency-graph

- scala official, how to use plugins: 
http://www.scala-sbt.org/0.13/docs/Using-Plugins.html

- SBT-dependency tree, October 14, 2014	by Satendra Kumar
https://blog.knoldus.com/2014/10/14/sbt-dependency-tree/
     
- Visualizing Project Dependencies in Sbt 
http://xerial.org/blog/2014/03/27/visualizing-project-dependencies-in-sbt/


# Add sbt-assembly plugin: TODO

## links:

- sbt-assembly migration: https://github.com/sbt/sbt-assembly/blob/master/Migration.md
- scala plugin bet practice: http://www.scala-sbt.org/0.13/docs/Plugins-Best-Practices.html