# Databricks notebook source
# MAGIC %md
# MAGIC # Question
# MAGIC You are given a time series data, which is clickstream of user activity. Perform Sessionization on the data and generate the session ids.
# MAGIC
# MAGIC Add an additional column with name session_id and generate the session ids based on --
# MAGIC 1. Session expires after inactivity of 30 minutes, no clickstream will be recorded in this.
# MAGIC 2. Session remain active for a max of 2 hours (i.e. after every 2 hours, a new session starts)

# COMMAND ----------

# MAGIC %md
# MAGIC # data
# MAGIC
# MAGIC Timestamp, User_id
# MAGIC
# MAGIC 2021-05-01T11:00:00Z, u1
# MAGIC
# MAGIC 2021-05-01T13:13:00Z, u1
# MAGIC
# MAGIC 2021-05-01T15:00:00Z, u2
# MAGIC
# MAGIC 2021-05-01T11:25:00Z, u1
# MAGIC
# MAGIC 2021-05-01T15:15:00Z, u2
# MAGIC
# MAGIC 2021-05-01T02:13:00Z, u3
# MAGIC
# MAGIC 2021-05-03T02:15:00Z, u4
# MAGIC
# MAGIC 2021-05-02T11:45:00Z, u1
# MAGIC
# MAGIC 2021-05-02T11:00:00Z, u3
# MAGIC
# MAGIC 2021-05-03T12:15:00Z, u3
# MAGIC
# MAGIC 2021-05-03T11:00:00Z, u4
# MAGIC
# MAGIC 2021-05-03T21:00:00Z, u4
# MAGIC
# MAGIC 2021-05-04T19:00:00Z, u2
# MAGIC
# MAGIC 2021-05-04T09:00:00Z, u3
# MAGIC
# MAGIC 2021-05-01T08:15:00Z, u1

# COMMAND ----------

# MAGIC %md
# MAGIC # Sample Output
# MAGIC
# MAGIC Timestamp, User_id, Session_id
# MAGIC
# MAGIC 2021-05-01T11:00:00Z, u1, u1_s1
# MAGIC
# MAGIC 2021-05-01T13:13:00Z, u1, u1_s2
# MAGIC
# MAGIC 2021-05-01T15:00:00Z, u2, u2_s1

# COMMAND ----------

# MAGIC %md
# MAGIC Also, consider the following scenario:
# MAGIC 1. Get number of sessions generated for each day
# MAGIC 2. Total time spent by a user in a day

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col,lag,lead,when,unix_timestamp,sum,concat,lit,to_date,count

# COMMAND ----------

# DBTITLE 1,Data
schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("User_id", StringType(), True)
])

data = [
("2021-05-01T11:00:00Z", "u1"),
("2021-05-01T13:13:00Z", "u1"),
("2021-05-01T15:00:00Z", "u2"),
("2021-05-01T11:25:00Z", "u1"),
("2021-05-01T15:15:00Z", "u2"),
("2021-05-01T02:13:00Z", "u3"),
("2021-05-03T02:15:00Z", "u4"),
("2021-05-02T11:45:00Z", "u1"),
("2021-05-02T11:00:00Z", "u3"),
("2021-05-03T12:15:00Z", "u3"),
("2021-05-03T11:00:00Z", "u4"),
("2021-05-03T21:00:00Z", "u4"),
("2021-05-04T19:00:00Z", "u2"),
("2021-05-04T09:00:00Z", "u3"),
("2021-05-01T08:15:00Z", "u1"),]

click_df =  spark.createDataFrame(data,schema)
click_df = click_df.withColumn("timestamp",col("timestamp").cast("timestamp"))

# COMMAND ----------

# DBTITLE 1,Tranformations
# Define time thresholds in seconds
inactivity_threshold = 30 * 60  # 30 minutes
max_session_duration = 2 * 60 * 60  # 2 hours

window_spec = Window.partitionBy("user_id").orderBy("Timestamp")

sess_df = click_df.withColumn("prev_timestamp",lag(col("timestamp"),1).over(window_spec))

sess_df = sess_df.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))

sess_df = sess_df.withColumn("new_session", when(
            (col("time_diff") >= inactivity_threshold) | (col("time_diff").isNull()) | (col("time_diff") > max_session_duration),
            1).otherwise(0))

# COMMAND ----------

window_spec_cumsum = Window.partitionBy("User_id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
# Use sum to create session IDs
sess_df = sess_df.withColumn("session_id", sum("new_session").over(window_spec_cumsum))

# Generate the final session identifier
sess_df = sess_df.withColumn("session_id", concat(col("user_id"), lit("_s"), col("session_id")))

# COMMAND ----------

# DBTITLE 1,output
sess_df.select("timestamp", "user_id", "session_id").display()

# COMMAND ----------

df = sess_df

# COMMAND ----------

# DBTITLE 1,Get number of sessions generated for each day
#Get number of sessions generated for each day 
df_day_sessions = df.withColumn("Date", to_date("Timestamp")).groupBy("Date", "User_id").agg(count("Session_id").alias("Num_Sessions"))
df_day_sessions.display()

# COMMAND ----------

# DBTITLE 1,Total time spent by a user in a day
# 2. Total time spent by a user in a day
# Calculate session duration by calculating the difference between session start and end
df_sessions = df.withColumn("Next_Timestamp", lead("Timestamp").over(window_spec))
df_sessions = df_sessions.withColumn("Session_Duration", 
                                     (col("Next_Timestamp").cast("long") - col("Timestamp").cast("long")) / 60)

# Calculate total time spent per user per day
df_sessions_day = df_sessions.withColumn("Date", to_date("Timestamp")) \
                              .groupBy("Date", "User_id") \
                              .agg(sum("Session_Duration").alias("Total_Time_Spent"))

df_sessions_day.display()
