# Skew

Data skew happens when each of the partitions have uneven distribution of data.

> In statistical terms, data is skewed when mean, median and mode aren't equal to each 
> other, as in the case with normal distribution.

## Symptoms

- Inconsistent processing times
- Strangler/frozen stages and tasks
- Low CPU utilization
- OOM errors

*Joins* and *Aggregations (group by)* are the scenarios where skewing can occur 
(mostly where the data needs to be shuffled i.e. 
records of the same key should be co-located in the same partition)

- too many `null` values in a key
- one key has very high cardinality. For e.g. one `product` is sold more than other products

## Solutions

### 1. Salting (Full or Partial)

#### Without salting

##### Code

`WhatIsTheAverageRevenueOfTheOrdersApp`

##### Job - Summary View

![](skewed-spark-app-summary.png)

##### Job - Details View

![](saprk-webui-jobs-view.png)

##### SQL Query View

(spark-webui-sql-view.pdf)

##### Join/Shuffle Stage - Details View

![](strangler-or-skewed-task-0.png)

##### Join/Shuffle Stage - Event Timeline View

![](strangler-or-skewed-task-1.png)

##### Join/Shuffle Stage - Task View

![](strangler-or-skewed-task-2.png)

##### Executor Memory

![](skewed-spark-app-GC-redflag.png)

#### With salting

- Had to increase the driver memory to `3G` in spark-shell to successfully run

```
./bin/spark-shell --driver-memory 3G
```

- `WhatIsTheAverageRevenueOfTheOrdersAppOptimized`

##### Iteration 1 - Replication factor 50

###### Job - Summary View

![]()

###### Job - Details View

![]()

###### SQL Query View

docs/skew/spark-webui-sql-view.pdf

###### Join/Shuffle Stage - Details View

![]()

###### Join/Shuffle Stage - Event Timeline View

![]()

###### Join/Shuffle Stage - Task View

![]()

###### Executor Memory

![]()

#### Iteration 2 - Replication factor 150 

###### Job - Summary View

![]()

###### Job - Details View

![]()

###### SQL Query View

docs/skew/spark-webui-sql-view.pdf

###### Join/Shuffle Stage - Details View

![]()

###### Join/Shuffle Stage - Event Timeline View

![]()

###### Join/Shuffle Stage - Task View

![]()

###### Executor Memory

![]()

## Reference

- https://dataengi.com/2019/02/06/spark-data-skew-problem/