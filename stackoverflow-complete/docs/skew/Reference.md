# Skew

## Table of Contents

1. [Introduction](#Introduction)
2. [Symptoms](#Symptoms)
3. [Solutions](#Solutions)
    - [Salting](#Salting)
        - [Without Salting](#WithoutSalting)
        - [With Salting](#WithSalting)
            - [Salting - Replication factor of 50](#SaltingWithReplicationFactor50)
            - [Salting - Replication factor of 150](#SaltingWithReplicationFactor150)
4. [Reference](#Reference)

## Introduction

Data skew happens when each of the partitions have uneven distribution of data i.e. non-uniform
data distribution between workers fo spark app.

> In statistical terms, data is skewed when mean, median and mode aren't equal to each 
> other, as in the case with normal distribution.

## Symptoms

- Inconsistent processing times
- Straggler/frozen stages and tasks
- Low CPU utilization
- OOM errors

*Joins* and *Aggregations (group by)* are the scenarios where skewing can occur 
(mostly where the data needs to be shuffled i.e. 
records of the same key should be co-located in the same partition)

- too many `null` values in a key
    - Preprocess the null values with some random id's and handle them in the application
- one key has very high cardinality. For e.g. one `product` is sold more than other products

## Solutions

> Databricks has a **skew** hint out of the box that cab avoid custom salting. Look at
> Reference section.

1. Data Preprocess
    - Repartition
2. Salting (Full & Partial)
3. Isolated Map Side Join
4. [Iterative Broadcast Join](https://www.youtube.com/watch?v=6zg7NTw-kTQ)

### Repartition

TODO

### Salting 

> For skewed data, shuffled data can be compressed heavily due to the repetitive nature of data. 
> Hence the overall disk IO/network transfer is also reduced. 

#### WithoutSalting

##### Code

[WhatIsTheAverageRevenueOfTheOrdersApp](../../src/main/scala/com/aravind/oss/eg/spark/sales/WhatIsTheAverageRevenueOfTheOrdersApp.scala)

##### Job - Summary View

![](skewed-spark-app-summary.png)

##### Job - Details View

![](saprk-webui-jobs-view.png)

##### SQL Query View

[SQL](spark-webui-sql-view.pdf)

##### Join/Shuffle Stage - Details View

![](strangler-or-skewed-task-0.png)

##### Join/Shuffle Stage - Event Timeline View

![](strangler-or-skewed-task-1.png)

##### Join/Shuffle Stage - Task View

![](strangler-or-skewed-task-2.png)

##### Executor Memory

![](skewed-spark-app-GC-redflag.png)

#### WithSalting

- Had to increase the driver memory to `3G` in spark-shell to successfully run

```
./bin/spark-shell --driver-memory 3G
```
##### Code

[WhatIsTheAverageRevenueOfTheOrdersAppOptimized](../../src/main/scala/com/aravind/oss/eg/spark/sales/WhatIsTheAverageRevenueOfTheOrdersAppOptimized.scala)

##### SaltingWithReplicationFactor50

###### Job - Summary View

![](./iteration1/skewed-spark-app-summary-optimize1.png)

###### Job - Details View

![](./iteration1/saprk-webui-jobs-view-optimize1.png)

###### SQL Query View

[SQL](./iteration1/spark-webui-sql-view-optimize1.pdf)

###### Join/Shuffle Stage - Details View

![](./iteration1/stranger-or-skewed-task-0-optimize1.png)

###### Join/Shuffle Stage - Event Timeline View

![](./iteration1/strangler-or-skewed-task-1-optimize1.png)

###### Join/Shuffle Stage - Task View

![](./iteration1/strangler-or-skewed-task-2-optimize1.png)

###### Executor Memory

![](./iteration1/skewed-spark-app-GC-redflag-optimize1.png)

#### SaltingWithReplicationFactor150

###### Job - Summary View

![](./iteration2/skewed-spark-app-summary-optimize2.png)

###### Job - Details View

![](./iteration2/saprk-webui-jobs-view-optimization2.png)

###### SQL Query View

[SQL](./iteration2/spark-webui-sql-view-optimization2.pdf)

###### Join/Shuffle Stage - Details View

![](./iteration2/stranger-or-skewed-task-0-optimize2.png)

###### Join/Shuffle Stage - Event Timeline View

![](./iteration2/strangler-or-skewed-task-1-optimize2.png)

###### Join/Shuffle Stage - Task View

![](./iteration2/strangler-or-skewed-task-2-optimize2.png)

###### Executor Memory

![](./iteration2/skewed-spark-app-GC-redflag-optimized2.png)

## Reference

- https://dataengi.com/2019/02/06/spark-data-skew-problem/
- https://dzone.com/articles/why-your-spark-apps-are-slow-or-failing-part-ii-da
- [Solution alternatives](https://bigdatacraziness.wordpress.com/2018/01/05/oh-my-god-is-my-data-skewed/)
- [Spark SQL hint](https://docs.databricks.com/delta/join-performance/skew-join.html#relation-columns-and-skew-values)
- [DataFrame hint](https://kb.databricks.com/data/skew-hints-in-join.html)