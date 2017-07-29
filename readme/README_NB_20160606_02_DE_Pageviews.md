# Power Plant ML Pipeline Application
## notebook link:

https://community.cloud.databricks.com/?o=8920468172695095#notebook/1809612576125024/command/1809612576125084

data: 
    download: https://datahub.io/en/dataset/english-wikipedia-pageviews-by-second
    
## summary:
- Objective: Analyze Desktop vs Mobile traffic to English Wikipedia
- Time to Complete: 30 mins
- Data Source: pageviews_by_second (255 MB)
- Business Questions:
    Question # 1) How many rows in the table refer to mobile vs desktop?
- Technical Accomplishments:
- Upload a file to Databricks using the Tables UI
- Learn how Actions kick off Jobs + Stages
- Understand how DataFrame partitions relate to compute tasks
- Use Spark UI to monitor details of Job execution (input read, Shuffle, Storage UI, SQL visualization)
- Cache a DataFrame to memory (and learn how to unpersist it)
- Use the following transformations: orderBy(), filter()
- Catalyst Optimizer: How DataFrame queries are converted from a Logical plan -> Physical plan
- Configuration Option: spark.sql.shuffle.partitions
