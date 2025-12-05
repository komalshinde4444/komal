from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("GlobalSuperstore").getOrCreate()

df = spark.read.csv(r"E:\GlobalSuperstore.csv", header=True, inferSchema=True)
df.show(5)

df = df.withColumn(
    "Sales",
    F.when(F.col("Sales").rlike("^[0-9]+(\\.[0-9]+)?$"), F.col("Sales").cast("double"))
     .otherwise(None)
)
df.createOrReplaceTempView("global")

#1️⃣ Total Sales by Customer top 5 customer
# spark.sql("""
#     select `Customer ID`, 
#            SUM(Sales) AS total
#     from global
#     group by `Customer ID`
#     order by total desc
#     limit 5
# """).show()

#2️⃣ Total Sales by Country
# spark.sql("""
#           select Country,sum(Sales) as total from global
#           group by Country
#           order by total desc
#           """).show()

#3️⃣ Average Sales per Category
# spark.sql("""
#           select Category, avg(Sales) as sale
#           from global
#           group by Category
#           order by sale desc 
#           """).show()

#4️⃣Top 5 Customers by Sales in Each Country
# spark.sql("""
#           select `Customer ID`, Country, total_sales,rnk from
#           (select `Customer ID`, Country,
#           SUM(Sales) as total_sales,
#           rank()
#           over( partition by Country order by SUM(Sales)  desc)as rnk
#           from global
#           group by `Customer ID`, Country) where rnk<=5
#           """).show()

#5️⃣ Total Sales by Ship Mode
# spark.sql("""
#           select `Ship Mode`,sum(Sales) as total,
#           rank() over(order by sum(Sales)) as rnk
#           from global
#           group by `Ship Mode` 
#           """).show()

# 6️⃣Category & Sub-Category Performance
# spark.sql("""
#           select Category , `Sub-Category`,round(sum(Sales),2) as total from global
#           group by Category , `Sub-Category`
#           order by total desc
#           """).show()

# 7️⃣Monthly Sales Trend
# spark.sql("""
#     SELECT 
#         date_format(try_to_date(`Order Date`, 'd/M/yyyy'), 'yyyy-MM') AS month,
#         ROUND(SUM(Sales), 2) AS total_sales
#     FROM global
#     WHERE try_to_date(`Order Date`, 'd/M/yyyy') IS NOT NULL
#     GROUP BY date_format(try_to_date(`Order Date`, 'd/M/yyyy'), 'yyyy-MM')
#     ORDER BY month
# """).show()

#8️⃣ Top 10 Products by Total Sales
spark.sql("""
          select `Product Name`,sum(Sales) as total_sales from global
          group by `Product Name`
          order by total_sales desc limit 10
          """).show()

