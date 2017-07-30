# 03-DA-Pageviews Application
## notebook link:

https://community.cloud.databricks.com/?o=8920468172695095#notebook/1809612576130134/command/1809612576130136

data: 
    download: https://datahub.io/en/dataset/english-wikipedia-pageviews-by-second
    
    wikipedia 2015 all data: 
        https://figshare.com/articles/Wikipedia_Clickstream/1305770
        https://datahub.io/dataset/wikipedia-clickstream/resource/be85cc68-d1e6-4134-804a-fd36b94dbb82
    
## summary:
Business Questions:
- Question # 1) How many total incoming requests were to the mobile site vs the desktop site?
- Question # 2) What is the start and end range of time for the pageviews data? How many days of data is in the DataFrame?
- Question # 3) What is the avg/min/max for the number of requests received for Mobile and Desktop views?
- Question # 4) Which day of the week does Wikipedia get the most traffic?
- Question # 5) Can you visualize both the mobile and desktop site requests in a line chart to compare traffic between both sites by day of the week?
- Question # 6) Why is there so much more traffic on Monday vs. other days of the week?

Technical Accomplishments:
- Give a DataFrame a human-readable name when caching
- Cast a String col type into a Timestamp col type
- Browse the Spark SQL API docs
- Learn how to use "Date time functions"
- Create and use a User Defined Function (UDF)
- Make a Databricks bar chart visualization
- Join 2 DataFrames
- Make a Matplotlib visualization