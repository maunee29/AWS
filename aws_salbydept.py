from pyspark.sql import SparkSession
import argparse

def salbysum(input_table, output_table):
    spark = SparkSession.builder.appName("RedshiftSalaryByDepartment").getOrCreate()

    # Read the data from Redshift table
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:redshift://redshift-cluster-1.ctahaomktoyn.us-east-2.redshift.amazonaws.com:5439/dev") \
        .option("dbtable", input_table) \
        .option("user", "redshift-cluster-1") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .load()

    # Group results by dept_name and calculate the sum of the salaries and order alphabetically
    sumDF = df.groupby("dept_name").sum("salary")
    sumDF = sumDF.orderBy("dept_name")

    # Write result to the specified output table
    sumDF.write \
        .format("jdbc") \
        .option("url", "jdbc:redshift://redshift-cluster-1.ctahaomktoyn.us-east-2.redshift.amazonaws.com:5439/dev") \
        .option("dbtable", output_table) \
        .option("user", "redshift-cluster-1") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Salary Count")
    parser.add_argument("input_table", help="Name of the Redshift Table to read data from")
    parser.add_argument("output_table", help="Name of the Redshift Table to write results to")

    # Parse the arguments
    args = parser.parse_args()

    # Call the salbysum function with the provided input table and output table
    salbysum(args.input_table, args.output_table)
