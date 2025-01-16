# Databricks notebook source
# MAGIC %md
# MAGIC # Question
# MAGIC Given a dataset with columns PERSON, TYPE, and AGE,
# MAGIC create an output where the oldest adult is paired with the youngest child, producing pairs of ADULT and CHILD while ensuring appropriate data matching.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 Check out the input and output in the table below!
# MAGIC
# MAGIC Input:--->
# MAGIC
# MAGIC | PERSON | TYPE | AGE |
# MAGIC | ------ | ------ | --- |
# MAGIC | A1 | ADULT | 54 |
# MAGIC | A2 | ADULT | 53 |
# MAGIC | A3 | ADULT | 52 |
# MAGIC | A4 | ADULT | 58 |
# MAGIC | A5 | ADULT | 54 |
# MAGIC | C1 | CHILD | 20 |
# MAGIC | C2 | CHILD | 19 |
# MAGIC | C3 | CHILD | 22 |
# MAGIC | C4 | CHILD | 15 |
# MAGIC
# MAGIC
# MAGIC Expected Output:--->
# MAGIC
# MAGIC | ADULT | CHILD |
# MAGIC | ----- | ----- |
# MAGIC | A4 | C4 |
# MAGIC | A5 | C2 |
# MAGIC | A1 | C1 |
# MAGIC | A2 | C3 |
# MAGIC | A3 | NULL |
# MAGIC '''

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Data
data = [    ('A1', 'ADULT', 54),
            ('A2', 'ADULT', 53),
            ('A3', 'ADULT', 52),
            ('A4', 'ADULT', 58),
            ('A5', 'ADULT', 54),
            ('C1', 'CHILD', 20),
            ('C2', 'CHILD', 19),
            ('C3', 'CHILD', 22),
            ('C4', 'CHILD', 15)
        ]
columns = ['person', 'type', 'age']
df =spark.createDataFrame(data, columns)

# COMMAND ----------

# DBTITLE 1,Transformations
adultDf = df.filter(col('type')=='ADULT') \
            .withColumn('rnk', row_number().over(Window.orderBy(desc('age'))))

childDf = df.filter(col('type')=='CHILD')\
                        .withColumn('rnk', row_number().over(Window.orderBy('age')))

resultDf = adultDf.alias('A').join(childDf.alias('C'), on='rnk', how='full')\
                        .select(col('A.person'),
                                col('C.person'))

# COMMAND ----------

# DBTITLE 1,Output
resultDf.display()
