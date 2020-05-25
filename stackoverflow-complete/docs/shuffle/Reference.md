# Shuffles

## Introduction

The shuffling is an expensive operation as it involves **Disk I/O, Data Serialization & Network I/O**. Operations that can cause shuffle are 

1. Repartition operations like `repartition` or `coalesce`
2. ByKey operations like `reduceByKey` or `aggregateByKey`
3. Join operations like `cogroup` or `join`
4. Aggregations with `groupBy`

- Operations that trigger shuffles use hashmaps or in-memory buffers data structures to group or sort
- Some operations use these data structures only reduce-side (`join`, `cogroup`, `groupByKey`) or both (`reduceByKey`, `aggregateByKey`)
- Spark **doesn't** have a spilling mechanism for shuffle like Hadoop
- Spark reduce phase **doesn't overlap** with map phase

Map-side

- Each map task writes one shuffle file (os disk buffer) per reduce task
- Thus there will be `M*R` shuffle files
- `spark.shuffle.compress` is used to enable compression
- Snappy (LZF can be configured using `spark.io.compress.codec`) is used to compress the map outputs by default

Reduce-side

- Spark requires all data to fit into memory of the corresponding reducer task (`reduceByKey`, `aggregateByKey`)
- Shuffle is a pull operation in Spark unlike Hadoop where it is a push operation
- Each reducer maintains a network buffer to fetch map outputs. Default size is `48 MB`. Can be configured using `spark.reducer.maxMbInFlight`


## Before optimization

![](jobs-before-optimize.png)

Jobs 5, 6, 7 and 8 are launched with lot of tasks against lot of empty partitions (Specifics shown below).
These empty tasks, though not doing any work, will add up to the overall processing time

1. Schedular delay
2. Task Deserialization Time

| Job # | Total Stages | Launched Tasks | Empty Tasks/Partitions | Records to process |
| ---- |---- |---- |---- |---- |
|5 |2 (1 skipped) |4 |3 |50 |
|6 |2 (1 skipped) |20 |19 |50 |
|7 |2 (1 skipped) |100 |96 |223 |
|8 |2 (1 skipped) |75 |71 |200  |

[Refer the query plan](query-plan-before-optimize.pdf)

## After optimization

![](jobs-after-optimize.png)

We can avoid these empty partitions by reducing the **shuffle partitions** by setting

```
spark.conf.set("spark.sql.shuffle.partitions", 1)
```
[Refer the query plan](query-plan-after-optimize.pdf)

## Reference

- [Optimizing Shuffle Performance in Spark Paper](https://people.eecs.berkeley.edu/~kubitron/courses/cs262a-F13/projects/reports/project16_report.pdf)
- https://medium.com/@foundev/you-won-t-believe-how-spark-shuffling-will-probably-bite-you-also-windowing-e39d07bf754e