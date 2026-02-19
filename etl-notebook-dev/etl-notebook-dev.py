import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 60)
print(f"ETL Job Started: {args['JOB_NAME']}")
print(f"Environment: DEV")
print(f"Timestamp: {datetime.now()}")
print("=" * 60)

# Create sample employee data
data = [
    (1, "John Doe", "Engineering", 75000),
    (2, "Jane Smith", "Marketing", 65000),
    (3, "Bob Johnson", "Engineering", 80000),
    (4, "Alice Williams", "HR", 60000),
    (5, "Charlie Brown", "Sales", 70000)
]

columns = ["emp_id", "name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

print("\n📊 Sample Employee Data:")
df.show()

# Transform data
df_transformed = df.withColumn("processed_at", current_timestamp()) \
                   .withColumn("environment", lit("DEV"))

# Calculate salary grade
from pyspark.sql.functions import when
df_transformed = df_transformed.withColumn("salary_grade",
    when(col("salary") >= 75000, "High")
    .when(col("salary") >= 65000, "Medium")
    .otherwise("Low")
)

print("\n✅ Transformed Data:")
df_transformed.show()

# Department statistics
dept_stats = df_transformed.groupBy("department") \
    .agg({"salary": "avg", "emp_id": "count"}) \
    .withColumnRenamed("avg(salary)", "avg_salary") \
    .withColumnRenamed("count(emp_id)", "employee_count")

print("\n📈 Department Statistics:")
dept_stats.show()

# Summary
print("\n" + "=" * 60)
print("ETL JOB SUMMARY")
print("=" * 60)
print(f"Total Records Processed: {df_transformed.count()}")
print(f"Departments: {df_transformed.select('department').distinct().count()}")
print(f"Job Status: SUCCESS ✅")
print("=" * 60)

job.commit()