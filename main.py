
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
from pyspark.sql import SparkSession

# create a SparkSession
spark = SparkSession.builder.master("local[*]").appName("dataframe").getOrCreate()

# define Oracle database connection properties
j_url = "jdbc:oracle:thin:@//192.168.0.170:1521/xe"
pro = {
    "user": "sys as SYSDBA",
    "password": "amol09",
    "driver": "oracle.jdbc.driver.OracleDriver"
}
# read data from Oracle database into Spark dataframe
df = spark.read.jdbc(url=j_url, table="hr.employees", properties=pro)
df.show(10)
