# Fabric migration issues and fixes

> Fabric Runtime 1.3

**Issue**

```
InvalidHttpRequestToLivy: [InvalidSparkSettings] Unable to submit this request because spark settings specified are 
invalid. Error: Code = SparkSettingsInvalidDriverMemory, Message = 'Invalid DriverMemory: '4g'. 
DriverMemory must be in 7g,14g,28g,56g,112g,224g,400g.'. Please correct the spark settings that 
align with your capacity Sku and SkuType and try again.. HTTP status code: 400.
```

_Solution_

Fixed after using 7g for driver memory.

**Issue**
```scala
java.lang.ClassCastException: class jdk.internal.loader.ClassLoaders$AppClassLoader cannot be cast to class java.net.URLClassLoader (jdk.internal.loader.ClassLoaders$AppClassLoader and java.net.URLClassLoader are in module java.base of loader 'bootstrap')
  at com.tccc.dna.synapse.spark.Classpath$.classpath$lzycompute(Classpath.scala:7)
  at com.tccc.dna.synapse.spark.Classpath$.classpath(Classpath.scala:7)
  at com.tccc.dna.synapse.spark.Classpath$.getAllDependencies(Classpath.scala:16)
  at com.tccc.dna.synapse.spark.Classpath$.getAllDependencyCount(Classpath.scala:14
  ```

_Solution_


**Issue**

_Solution_