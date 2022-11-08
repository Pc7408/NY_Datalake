# Databricks notebook source
from pyspark.sql.functions import split, explode
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import regexp_replace
import sys


spark = SparkSession.builder.appName('NewYorker_Datalake').getOrCreate()
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# COMMAND ----------

spark.sql("create database if not exists yelp_acadamic_data_raw")


# COMMAND ----------
business_file=sys.argv[0]
checkin_file=sys.argv[1]
review_file=sys.argv[2]
tip_file=sys.argv[3]
user_file=sys.argv[4]
#BUsiness file
file_location = business_file
# The applied options are for CSV files. For other file types, these will be ignored.
df_business = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#display(df_business)
df_business.write.mode("overwrite").saveAsTable("yelp_academic_dataset_business_json")


# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_raw.yelp_academic_dataset_business_raw as
select * from yelp_academic_dataset_business_json""")


# COMMAND ----------

#Checkin file
file_location = checkin_file
# The applied options are for CSV files. For other file types, these will be ignored.
df_checkin = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df_checkin.write.mode("overwrite").saveAsTable("yelp_academic_dataset_checkin_json")

# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_raw.yelp_academic_dataset_checkin_raw as
select * from yelp_academic_dataset_checkin_json""")


# COMMAND ----------

#review Data
file_location = review_file
# The applied options are for CSV files. For other file types, these will be ignored.
df_review = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df_review.write.mode("overwrite").saveAsTable("yelp_academic_dataset_review_json")

# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_raw.yelp_academic_dataset_review_raw as 
select * from yelp_academic_dataset_review_json""")


# COMMAND ----------

#Tip Data
file_location = tip_file
# The applied options are for CSV files. For other file types, these will be ignored.
df_tip = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df_tip.write.mode("overwrite").saveAsTable("yelp_academic_dataset_tip_json")

# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_raw.yelp_academic_dataset_tip_raw as 
select * from yelp_academic_dataset_tip_json""")


# COMMAND ----------

#User Data
file_location = user_file
df_user = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df_user.write.mode("overwrite").saveAsTable("yelp_academic_dataset_user_json")

# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_raw.yelp_academic_dataset_user_raw as 
select * from yelp_academic_dataset_user_json""")


# COMMAND ----------

spark.sql("""drop database yelp_acadamic_data_cleaned cascade""")

# COMMAND ----------

spark.sql("""create database if not exists yelp_acadamic_data_cleaned""")

# COMMAND ----------

df_business_cleaned=df_business.select("*", regexp_replace("business_id", "[^a-zA-Z0-9]", "").alias('business_id1'))
df_business_cleaned=df_business_cleaned.drop('business_id').withColumnRenamed('business_id1','business_id')
df_business_cleaned.write.mode("overwrite").saveAsTable("yelp_academic_dataset_business_clean")

# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_cleaned.yelp_academic_dataset_business_clean as 
select * from yelp_academic_dataset_business_clean""")


# COMMAND ----------

df_checkin_cleaned=df_checkin.select("*", regexp_replace("business_id", "[^a-zA-Z0-9]", "").alias('business_id1'))
df_checkin_cleaned=df_checkin_cleaned.drop('business_id').withColumnRenamed('business_id1','business_id')
df_checkin_cleaned.write.mode("overwrite").saveAsTable("yelp_academic_dataset_checkin_cleaned")

# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_cleaned.yelp_academic_dataset_checkin_clean as
select * from yelp_academic_dataset_checkin_cleaned""")


# COMMAND ----------

df_review_cleaned=df_review.select("*", regexp_replace("business_id", "[^a-zA-Z0-9]", "").alias('business_id1'),regexp_replace("review_id", "[^a-zA-Z0-9]", "").alias('review_id1'),regexp_replace("user_id", "[^a-zA-Z0-9]", "").alias('user_id1'))
df_review_cleaned=df_review_cleaned.drop('business_id','review_id','user_id').withColumnRenamed('business_id1','business_id').withColumnRenamed('review_id1','review_id').withColumnRenamed('user_id1','user_id')
df_review_cleaned.write.mode("overwrite").saveAsTable("yelp_academic_dataset_review_cleaned")


# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_cleaned.yelp_academic_dataset_review_clean as 
select * from yelp_academic_dataset_review_cleaned""")


# COMMAND ----------

df_tip_cleaned=df_tip.select("*",regexp_replace("business_id", "[^a-zA-Z0-9]", "").alias('business_id1'),regexp_replace("user_id", "[^a-zA-Z0-9]", "").alias('user_id1'))
df_tip_cleaned=df_tip_cleaned.drop('business_id','user_id').withColumnRenamed('business_id1','business_id').withColumnRenamed('user_id1','user_id')
df_tip_cleaned.write.mode("overwrite").saveAsTable("yelp_academic_dataset_tip_cleaned")



# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_cleaned.yelp_academic_dataset_tip_clean as 
select * from yelp_academic_dataset_tip_cleaned""")


# COMMAND ----------

df_user_cleaned=df_user.select("*",regexp_replace("user_id", "[^a-zA-Z0-9]", "").alias('user_id1'))
df_user_cleaned=df_user_cleaned.drop('user_id').withColumnRenamed('user_id1','user_id')
df_user_cleaned.write.mode("overwrite").saveAsTable("yelp_academic_dataset_user_cleaned")

# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_cleaned.yelp_academic_dataset_user_clean as 
select * from yelp_academic_dataset_user_cleaned""")



# COMMAND ----------

spark.sql("""create database if not exists yelp_acadamic_data_insight""")


# COMMAND ----------

df_review_aggregate=df_review_cleaned.withColumn("week_of_year", weekofyear(col("date")))
windowSpec = Window.partitionBy("business_id").orderBy("week_of_year")
df_star_per_business_on_weekly=df_review_aggregate.withColumn("row_number",f.row_number().over(windowSpec)).select('stars','business_id','week_of_year')
df_star_per_business_on_weekly.write.mode("overwrite").saveAsTable("star_per_business_on_weekly")


# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_insight.star_per_business_on_weekly as 
select * from star_per_business_on_weekly""")

# COMMAND ----------

df_checkin_of_business=df_review_aggregate.join(df_checkin_cleaned,
                                    df_review_aggregate.business_id==df_checkin_cleaned.business_id,"inner").\
select(df_review_aggregate.business_id,df_review_aggregate.stars,size(split(df_checkin_cleaned.date,",")).alias("no_of_checkin"))

df_checkin_of_business.write.mode("overwrite").saveAsTable("checkin_of_business_on_stars")

# COMMAND ----------

spark.sql("""create table yelp_acadamic_data_insight.checkin_of_business_on_stars as 
select * from checkin_of_business_on_stars""")

# COMMAND ----------

spark.sql("""select * from yelp_acadamic_data_insight.star_per_business_on_weekly""").show(100,False)
spark.sql("""select * from yelp_acadamic_data_insight.checkin_of_business_on_stars""").show(100,False)
