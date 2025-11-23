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

## Repartition vs Coalesce

<img width="900" height="414" alt="image" src="https://github.com/user-attachments/assets/643a141e-b5de-471f-800a-024a934c9941" />

Repartition vs. Coalesce in PySpark: Stop Guessing, Start Choosing

Both repartition() and coalesce() are fundamental PySpark methods used to manage the number of partitions in a Resilient Distributed Dataset (RDD) or DataFrame. However, they employ radically different mechanisms, trading shuffle cost for data balance in distinct ways. Understanding this trade-off is critical for optimizing performance, managing cluster resources, and ensuring data integrity at scale.

Feature Data Shuffle Partition Size Use Case Performance Impact 

df.repartition(N) 
Full Shuffle. Data is redistributed across the entire cluster.
Evenly Distributed. Attempts to create partitions of roughly equal size.
Ideal for increasing partitions or fixing existing skew/imbalance.
Higher CPU/Network overhead due to full data movement.

df.coalesce(N)
Minimal Shuffle. Moves data only when necessary, often stitching adjacent partitions together.
Uneven Distribution. Partitions may have vastly different sizes, preserving existing data skew.
Ideal for decreasing partitions efficiently, especially for final writes.
Lower CPU/Network overhead; faster execution if original partitions were already balanced.

Key Scenarios for Repartition:

Fixing Data Skew: If a previous transformation (like a groupBy or join) introduced heavy skew, repartition() must be used to redistribute the data evenly before subsequent wide transformations.

Increasing Parallelism: When transitioning from a light operation to a heavy operation (e.g., intensive aggregations or complex UDFs), use repartition() to increase the number of partitions beyond the default to leverage more cluster cores.

Optimal Writes: Before writing to storage, particularly when writing to a sink like Azure Synapse or Delta Lake, repartition() ensures data is uniformly spread, preventing resource contention on specific nodes and leading to more predictable write times.

Partition Key Specification: You can use df.repartition(N, col1, col2) to redistribute the DataFrame based on specific columns, ensuring records with the same key are always processed by the same executor.

Key Scenarios for Coalesce:
Reducing File Count: After filtering out a large portion of data, many partitions may become very small. coalesce() is excellent for consolidating these into fewer, larger partitions before writing, thereby avoiding the small file problem (overhead from too many file system operations).

Minimal Shuffle Requirement: When a job is time-critical, and the current partition count is higher than required for the final step (but the data is generally balanced), coalesce() minimizes the network I/O and overhead associated with a full shuffle.

Shrink and Write: It is best practice for the final step of an ETL/ELT pipeline where the goal is to output a manageable number of files efficiently.

One-Liner Decision Matrix:
Use repartition().
Need to quickly reduce partition count with minimal shuffle cost? Use coalesce().


## Reference
- [An In-Depth Look at Spark Partitioning Strategies](https://medium.com/airbnb-engineering/on-spark-hive-and-small-files-an-in-depth-look-at-spark-partitioning-strategies-a9a364f908)
