# We create a spark session.

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ETLProject").getOrCreate()

# We read the .csv file.

df = spark.read.format("csv").option("header",True).load("dbfs:/FileStore/tables/ds_salaries.csv")
df.limit(5).display()

# We check that whether there are any nulls in each column.
for column in df.columns:
    print(f"Number of null values in column {column}:")
    print(df.where(df[column].isNull()).count())

# We import the necessary libraries to built a schema
from pyspark.sql.types import StructType 
from pyspark.sql.types import StructField 
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType

ds_schema = StructType([
    StructField("work_year",IntegerType(), True),
    StructField("experience_level",StringType(), True),
    StructField("employment_type",StringType(), True),
    StructField("job_title",StringType(), True),
    StructField("salary",IntegerType(), True),
    StructField("salary_currency",StringType(), True),
    StructField("salary_in_usd",IntegerType(), True),
    StructField("employee_residence",StringType(), True),
    StructField("remote_ratio",StringType(), True),   
    StructField("company_location",StringType(), True),
    StructField("company_size",StringType(), True)
])
df = spark.read.csv("dbfs:/FileStore/tables/ds_salaries.csv", header=True, schema=ds_schema)
df.limit(8).display()

df.printSchema()

# We transfrom the "remote_ratio" column as "working_type" column.
# To do that an udf(user-defined function) should be used.
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def working_type(x):
    if x == "0":
        return "on-site"
    elif x == "50":
        return "hybrid"
    elif x == "100":
        return "remote"

working_type_udf = udf(working_type, StringType())
df = df.withColumn("working_type", working_type_udf("remote_ratio"))

df.display()

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import avg

def format_salary(avg_salary):
    return f"{int(avg_salary/1000)}K"

year_df = df.groupBy("work_year").agg(avg("salary").alias("avg_salary"))
format_salary_udf = udf(format_salary, StringType())
year_df = year_df.withColumn("usd_avg_salary_by_year", format_salary_udf("avg_salary"))
year_df.display()

df = df.join(year_df, on="work_year", how="left")
df.display()

from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id

df = df.select(lit(monotonically_increasing_id()).alias("employee_id"), "*")
df.display()

from pyspark.sql.functions import when

# This column indicates whether the salary of the employee is below or above the average salary of the year.
df = df.withColumn(
    "salary_comparison_by_yearly_avg",
    when(df["salary"] >= df["avg_salary"], "above").otherwise("below")
)
df.display()

df = df.drop("remote_ratio", "avg_salary")

df.display()

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def employment_type(x):
    if x == "FT":
        return "Full-Time"
    elif x == "PT":
        return "Part-Time"
    elif x == "CT":
        return "Contract"
    elif x == "FL":
        return "Freelance"


employment_type_udf = udf(employment_type, StringType())
df = df.withColumn("employment_type", employment_type_udf("employment_type"))

df.display()

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def experience_level(x):
    if x == "EX":
        return "Expert"
    elif x == "MI":
        return "Mid-level"
    elif x == "EN":
        return "Entry-level"
    elif x == "SE":
        return "Senior-level"


experience_level_udf = udf(experience_level, StringType())
df = df.withColumn("experience_level", experience_level_udf("experience_level"))

df.display()

column_list = ["work_year",
"experience_level",
"employment_type",
"job_title",
"salary_currency",
"employee_residence",
"company_location",
"company_size"]
# We display the different values that some columns have.
for column in column_list:
    df.select(column).distinct().show()

# We save the dataframe to a database.

df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://35.202.80.66:5432/postgres")\
    .option("driver", "org.postgresql.Driver").option("dbtable", "ds_salaries")\
    .option("user", "postgres").option("password", "postgres")\
    .mode("append").save()
