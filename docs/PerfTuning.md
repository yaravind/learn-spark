# Performance Tuning

## Training

1. [Spark Query Plans](https://www.youtube.com/watch?v=KnUXztKueMU)
2. [Spark DAGs](https://www.youtube.com/watch?v=O_45zAz1OGk)
3. [Spark Memory Management](https://www.youtube.com/watch?v=sXL1qgrPysg)
4. [Spark Executor Tuning](https://www.youtube.com/watch?v=mA96gUESVZc)
5. [Shuffle Partitions](https://www.youtube.com/watch?v=q1LtBU_ca20)
6. [Data Partitioning](https://www.youtube.com/watch?v=fZndmQasykk)
7. [Bucketing](https://www.youtube.com/watch?v=1kWl6d1yeKA)
8. [Caching](https://www.youtube.com/watch?v=FujwRYkBwM4)
9. [Data Skew](https://www.youtube.com/watch?v=9Ss-_y7njKE)
10. [Salting](https://www.youtube.com/watch?v=rZGsc5y8AQk)
11. [AQE & Broadcast Joins](https://www.youtube.com/watch?v=bRjVa7MgsBM)
12. [Dynamic Partition Pruning](https://www.youtube.com/watch?v=s8Z8Gex7VFw)

[Link to](https://github.com/afaqueahmad7117/spark-experiments) code repo for above training.

## Examples

### Example 1

Cluster

- Number of data-nodes: 12
- Number of racks: 2

```
spark-shell --master yarn --packages com.databricks:spark-xml_2.11:0.5.0 --deploy-mode client
```
As you can see below, the default resources allocated are

- Driver: 394.1 MB RAM
- Total Executors: 2 
- Each executor: 1 core, 384.1 MB RAM 

![spark-config-iteration1](spark-shell-iteration1.png)

---

```
spark-shell --master yarn --packages com.databricks:spark-xml_2.11:0.5.0 --deploy-mode client --executor-memory 8G --num-executors 12
```

**comments.count = 6.4 mins with 142 partitions**

As you can see below, the resources allocated are

- Driver: 394.1 MB RAM
- Total Executors: *12*
- Each executor: 1 core, 4.4 GB RAM 

![spark-config-iteration1](spark-shell-iteration2.png)

---

```
spark-shell --master yarn --packages com.databricks:spark-xml_2.11:0.5.0 --deploy-mode client --executor-memory 8G --num-executors 24
```

**comments.count = 3.3 min mins with 142 partitions**

As you can see below, the resources allocated are

- Driver: 394.1 MB RAM
- Total Executors: *24* 
- Each executor: 1 core, 4.4 GB RAM 

![spark-config-iteration1](spark-shell-iteration3.png)