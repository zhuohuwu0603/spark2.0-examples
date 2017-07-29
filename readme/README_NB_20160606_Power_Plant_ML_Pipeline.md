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
                
        If want more accuracy: uncomment dt.setMaxBins(10) with dt.setMaxBins(100)
         
         and     //gbt.setMaxBins(100)
                 //gbt.setMaxIter(120)
                 
         It will take about 9 minutes to finish. 
            
        
## add net.liftweb	lift-json_2.11 as external jar  (optional), just show how to add a jar dependency
        
IntelliJ IDEA 15 & 2016
- File > Project Structure... or press Ctrl + Alt + Shift + S.
- Project Settings > Modules > Dependencies > "+" sign > JARs or directories...
- Select the jar file and click on OK, then click on another OK button to confirm.
- You can view the jar file in the "External Libraries" folder.        
