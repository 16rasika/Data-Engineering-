# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark import SparkConf
spark=SparkSession.builder.appName('Test').getOrCreate()
spark
from pyspark.sql.functions import avg,count,desc,when,col,length

# COMMAND ----------

#importing csv
df1= spark.read.csv('amazon.csv',header=True,inferSchema=True)
#df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/16rasika@gmail.com/amazon-1.csv")

# COMMAND ----------

#look into csv
df1.show()

# COMMAND ----------

# more detail view like pandas
df1.display()

# COMMAND ----------

# looking datatype 
# all data type are string bcoz when importing csv forgot to mention inferSchema= True
df1.printSchema()

# COMMAND ----------

#df1.select('product_name').show()
df1.select('product_name').display()

# COMMAND ----------

# top product by rating
top_rated_product =df1.groupby('product_id','product_name').agg(avg('rating').alias('avg_rating')).orderBy(desc('avg_rating')).limit(10) 

# COMMAND ----------

top_rated_product.display()

# COMMAND ----------

#top reviewed product
top_reviewed_product = df1.groupby('product_id','product_name').count().orderBy(desc('count')).limit(10)
top_reviewed_product.display()

# COMMAND ----------

#discount analysis

discount_analysis = df1.groupby('category').agg(avg('discount_percentage').alias('avg_discount'))
discount_analysis.display()

# COMMAND ----------

# user engagement
user_engagement = df1.groupBy('product_id','product_name').agg(avg('rating').alias('avg_rating'),count('rating').alias('rating_count'))
user_engagement.display()

# COMMAND ----------

#creating temp table from DF1
df1.createOrReplaceTempView('amazon_sales_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amazon_sales_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from amazon_sales_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amazon_sales_table order by product_id desc limit 10

# COMMAND ----------


