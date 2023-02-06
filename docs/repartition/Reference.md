# Repartition

Use **coalesce** if:
- You're writing fewer files than your sPartition count
- You can bear to perform a cache and count operation before your coalesce
- You're writing exactly 1 hPartition

Use **simple repartition** if:
- You're writing exactly 1 hPartition
- You can't use coalesce

Use a **simple repartition by columns** if:
- You're writing multiple hPartitions, but each hPartition needs exactly 1 file
- Your hPartitions are roughly equally sized

Use a **repartition by columns with a random factor** if:
- Your hPartitions are roughly equally sized
- You feel comfortable maintaining a files-per-hPartition variable for this dataset
- You can estimate the number of output hPartitions at runtime or, you can guarantee your default parallelism will always be much larger (~3x) your output file count for any dataset you're writing. Use a repartition by range (with the hash/rand columns) in every other case.

## Reference
- [An In-Depth Look at Spark Partitioning Strategies](https://medium.com/airbnb-engineering/on-spark-hive-and-small-files-an-in-depth-look-at-spark-partitioning-strategies-a9a364f908)