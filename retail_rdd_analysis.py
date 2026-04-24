from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RetailRDD").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile("retail_data.csv")

header = rdd.first()
data = rdd.filter(lambda x: x != header)

rows = data.map(lambda x: x.split(","))

# Revenue = qty * price
sales = rows.map(lambda x: (x[2], int(x[4]) * float(x[5])))

# Total sales by product
product_sales = sales.reduceByKey(lambda a, b: a + b)

# Region revenue
region_sales = rows.map(lambda x: (x[6], int(x[4]) * float(x[5]))).reduceByKey(lambda a,b:a+b)

# Customer orders count
customer_orders = rows.map(lambda x: (x[1],1)).reduceByKey(lambda a,b:a+b)

print("Top Product Sales:")
for i in product_sales.collect():
    print(i)

print("\nRegion Revenue:")
for i in region_sales.collect():
    print(i)

print("\nRepeat Customers:")
for i in customer_orders.filter(lambda x: x[1] > 1).collect():
    print(i)

spark.stop()
