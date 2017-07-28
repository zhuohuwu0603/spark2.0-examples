# Examples for Spark 2.0 release.
## Convert the notebook to local run code in intellij for structural streaming  

https://www.youtube.com/watch?v=_1byVWTEK1s
https://community.cloud.databricks.com/?o=8920468172695095#notebook/3588089277658157/command/3588089277658189

## 
Inputfile: 
        spark2.0-examples/src/main/resources/stream/in/d1.txt
Output file:
        spark2.0-examples/src/main/resources/stream/ck
        spark2.0-examples/src/main/resources/stream/out/_spark_metadata
        
local: 

    cd /Users/kevinwu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples
    echo -e '{"latname":"Jones", "email":"Jones@gmail.com", "hits":3}\n{"latname":"Smith", "email":"smith@gmail.com", "hits":5}' >> src/main/resources/stream/in/d1.txt