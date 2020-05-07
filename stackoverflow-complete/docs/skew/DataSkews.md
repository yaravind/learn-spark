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
- one key has very high cardinality. For e.g. one `product` is sold more than any other
products

## Examples

### Salting (Full or Partial)

#### Without salting
    - `WhatIsTheAverageRevenueOfTheOrdersApp` -

#### With salting

- Had to increase the driver memory to `3G` in spark-shell to successfully run
- `WhatIsTheAverageRevenueOfTheOrdersAppOptimized`

##### Iteration 1 - Replication factor 50



#### Iteration 2 - Replication factor 150 

## Reference

- https://dataengi.com/2019/02/06/spark-data-skew-problem/