# Power Plant ML Pipeline Application
## notebook link:

https://community.cloud.databricks.com/?o=8920468172695095#notebook/1809612576125024/command/1809612576125084

## summary:
This is an end-to-end example of using a number of different machine learning algorithms to solve a supervised regression problem.
Table of Contents
- Step 1: Business Understanding
- Step 2: Load Your Data
- Step 3: Explore Your Data
- Step 4: Visualize Your Data
- Step 5: Data Preparation
- Step 6: Data Modeling
- Step 7: Tuning and Evaluation
- Step 8: Streaming Deployment

## 
Inputfile: 
        
        downloaded from: https://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant
        put it in: spark2.0-examples/src/main/resources/power-plant/data/Folds5x2_pp.xlsx
        
Output file:
        spark2.0-examples/src/main/resources/stream/ck
        spark2.0-examples/src/main/resources/stream/out/_spark_metadata
        
net.liftweb	lift-json_2.11		Select        
        
local: 

    cd /Users/kevinwu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples
    echo -e '{"latname":"Jones", "email":"Jones@gmail.com", "hits":3}\n{"latname":"Smith", "email":"smith@gmail.com", "hits":5}' >> src/main/resources/stream/in/d1.txt