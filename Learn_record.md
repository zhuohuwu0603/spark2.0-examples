
# 20161120

## sbt Assembly merge strategy error

### run: sbt assembly
/Users/kevinwu/.ivy2/cache/org.datanucleus/datanucleus-api-jdo/jars/datanucleus-api-jdo-3.2.6.jar:plugin.xml
/Users/kevinwu/.ivy2/cache/org.datanucleus/datanucleus-rdbms/jars/datanucleus-rdbms-3.2.9.jar:plugin.xml
        at sbtassembly.Assembly$.applyStrategies(Assembly.scala:140)
        at sbtassembly.Assembly$.x$1$lzycompute$1(Assembly.scala:25)
        at sbtassembly.Assembly$.x$1$1(Assembly.scala:23)
        at sbtassembly.Assembly$.stratMapping$lzycompute$1(Assembly.scala:23)
        at sbtassembly.Assembly$.stratMapping$1(Assembly.scala:23)
        at sbtassembly.Assembly$.inputs$lzycompute$1(Assembly.scala:67)
        at sbtassembly.Assembly$.inputs$1(Assembly.scala:57)
        at sbtassembly.Assembly$.apply(Assembly.scala:83)
        at sbtassembly.Assembly$$anonfun$assemblyTask$1.apply(Assembly.scala:240)
        at sbtassembly.Assembly$$anonfun$assemblyTask$1.apply(Assembly.scala:237)
        at scala.Function1$$anonfun$compose$1.apply(Function1.scala:47)
        at sbt.$tilde$greater$$anonfun$$u2219$1.apply(TypeFunctions.scala:40)
        at sbt.std.Transform$$anon$4.work(System.scala:63)
        at sbt.Execute$$anonfun$submit$1$$anonfun$apply$1.apply(Execute.scala:226)
        at sbt.Execute$$anonfun$submit$1$$anonfun$apply$1.apply(Execute.scala:226)
        at sbt.ErrorHandling$.wideConvert(ErrorHandling.scala:17)
        at sbt.Execute.work(Execute.scala:235)
        at sbt.Execute$$anonfun$submit$1.apply(Execute.scala:226)
        at sbt.Execute$$anonfun$submit$1.apply(Execute.scala:226)
        at sbt.ConcurrentRestrictions$$anon$4$$anonfun$1.apply(ConcurrentRestrictions.scala:159)
        at sbt.CompletionService$$anon$2.call(CompletionService.scala:28)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)
[error] (*:assembly) deduplicate: different file contents found in the following:
[error] /Users/kevinwu/.ivy2/cache/javax.inject/javax.inject/jars/javax.inject-1.jar:javax/inject/Inject.class
[error] /Users/kevinwu/.ivy2/cache/org.glassfish.hk2.external/javax.inject/jars/javax.inject-2.4.0-b34.jar:javax/inject/Inject.class
[error] deduplicate: different file contents found in the following:
... 

### Solution: 




# Problems

## Can't have both StreamingTest.scala and WordCounterTest.scala running. 