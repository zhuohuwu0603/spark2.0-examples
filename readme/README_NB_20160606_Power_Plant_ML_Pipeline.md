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

## Result:

- small iteration (10 iterations)
```
LR: 
    Root Mean Squared Error1: 4.5411092924667
    Explained Variance1: 272.86639197067933
    R2_1: 0.9300671383202985

Tuned LR: 
    Root Mean Squared Error2: 4.541393856591181
    Explained Variance2: 272.5921845811232
    R2_2: 0.9300583734984044
DT:
    Root Mean Squared Error3: 5.232606511264802
    Explained Variance3: 266.0281473341642
    R2_3: 0.9071475118358783    

GBT:         
    Root Mean Squared Error4: 4.754059362101928
    Explained Variance4: 275.66518968018397
    R2_4: 0.923354512301011            
```

- large iterations (120 iterations)
```
LR: 
    Root Mean Squared Error1: 4.5411092924667
    Explained Variance1: 272.86639197067933
    R2_1: 0.9300671383202985

Tuned LR: 
    Root Mean Squared Error2:  4.541393856591015
    Explained Variance2: 272.59218458111656
    R2_2: 0.9300583734984095
DT:
    Root Mean Squared Error3: 5.131856718248588
    Explained Variance3: 266.916192715665
    R2_3: 0.9106886948232996    

GBT:         
    Root Mean Squared Error4: 3.6127999377652795
    Explained Variance4: 279.69829188010834
    R2_4: 0.9557365544772013            
```

            
        
## add net.liftweb	lift-json_2.11 as external jar  (optional), just show how to add a jar dependency
        
IntelliJ IDEA 15 & 2016
- File > Project Structure... or press Ctrl + Alt + Shift + S.
- Project Settings > Modules > Dependencies > "+" sign > JARs or directories...
- Select the jar file and click on OK, then click on another OK button to confirm.
- You can view the jar file in the "External Libraries" folder.        
