from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, unix_timestamp, round

spark = SparkSession.builder.appName("KPI_Pipeline").getOrCreate()

attendance_df = spark.read.csv("/Volumes/workspace/default/nithyashree/attendance_logs.csv", header=True, inferSchema=True)
tasks_df = spark.read.csv("/Volumes/workspace/default/nithyashree/tasks.csv", header=True, inferSchema=True)

attendance_df = attendance_df.withColumn("workhours", round((unix_timestamp("clockout") - unix_timestamp("clockin")) / 3600, 2))

combined_df = attendance_df.join(tasks_df, on="employeeid", how="inner")

department_kpis = combined_df.groupBy("department").agg(
    avg("workhours").alias("avg_workhours"),
    avg("taskscompleted").alias("avg_tasks_completed"),
    count(col("employeeid")).alias("records_count")
)

department_kpis.write.mode("overwrite").parquet("/Volumes/workspace/default/nithyashree/sample_output_metrics.parquet")

department_kpis.show()
