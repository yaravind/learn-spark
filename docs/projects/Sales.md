# Sales Analytics

## Product

```
scala> productDF.printSchema
root
 |-- product_id: string (nullable = true)
 |-- product_name: string (nullable = true)
 |-- price: string (nullable = true)
```

## Sales

```
scala> orderDF.printSchema
root
 |-- order_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- seller_id: string (nullable = true)
 |-- date: string (nullable = true)
 |-- num_pieces_sold: string (nullable = true)
 |-- bill_raw_text: string (nullable = true)
```

## Sellers

```
scala> sellerDF.printSchema
root
 |-- seller_id: string (nullable = true)
 |-- seller_name: string (nullable = true)
 |-- daily_target: string (nullable = true)
```