Examples for Spark 2.0 release.

# Build

    sbt clean package
    
    
# Add test
    - cmd + shift + T: create new test cases (it will auto-create the folder structure in test, if there is problem, run sbt clean and re-create)
    
# Add coverageReport
    - run the command: sbt clean coverage test coverageReport
    - report output can be seen: /Users/kevinwu/Documents/zw_codes/GitLab/spark2.0-examples/target/scala-2.11/scoverage-report/index.html 
     