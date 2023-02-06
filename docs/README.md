# PBL (Project Based Learning) approach to learning Apache Spark

> **Tutorials are great, but building projects is the best way to learn.**

## Projects

|#|Project|What you will learn?|Status|
|-----|----|-------|-------|
|1|[Word Count](../docs/projects/WordCount.md)|Solve the same problem with different Spark API|Complete|
|2|[Sales Analytics](../docs/projects/Sales.md) |Identify and fix data Skew, Salting|In progress|
|3|[Users & Departments]()|Identify and fix data Skew, Repartition| Complete|
|3|[Stackoverflow Analytics](../docs/projects/Stackoverflow.md)|[Who earned the first badge?](https://www.youtube.com/watch?v=6nEBu6CtUng) |TODO|
|4|[Protect Data Cardinality](https://www.linkedin.com/pulse/protecting-your-data-cardinality-michael-spector/)| |TODO|

## Notes

1. [Data Skews](../docs/skew/Reference.md)
2. [Repartition](../docs/repartition/Reference.md)
2. [Performance Tuning](../docs/PerfTuning.md)
3. [Task Serialization](../docs/serialize/Reference.md)
3. [Garbage Collection](../docs/garbage-collection/Reference.md)

### 3 Spark SQL function types

| # | Name| Input| Output| Examples|
|---|-----|------|-------|---------|
|1| UDF or built-in functions|**Single** row | Single return value for *input row*| `round`, `substr`|
|2| Aggregate functions| **Group** of rows| Single return value for for *every group*| `avg`, `min` |
|3| Window functions| **Group** of rows| Single value for every *input row*| ranking etc. |

### Additional Joins

- Point-in-time join
- Range join
- Multi-dimensional index
Reference: https://www.waitingforcode.com/apache-spark/apache-spark-you-dont-know-it/read

## References

Shuffling

- http://hydronitrogen.com/apache-spark-shuffles-explained-in-depth.html

Windowing

- https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
