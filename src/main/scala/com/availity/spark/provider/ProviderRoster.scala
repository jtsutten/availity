package com.availity.spark.provider

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, count, lit, month}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ProviderRoster  {
  val spark = SparkSession.builder()
    .appName("Provider Visits Analysis")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  def process(providersDF: DataFrame, visitsDF: DataFrame): (DataFrame, DataFrame) = {
    val joinedDF = visitsDF.join(providersDF, "provider_id")

    val totalVisitsPerProvider = joinedDF
      .groupBy("provider_id", "provider_specialty", "first_name", "middle_name", "last_name")
      .agg(count("visit_id").alias("total_visits"))

    totalVisitsPerProvider.write
      .partitionBy("provider_specialty")
      .json("output/total_visits_per_provider")

    // Task 2: Calculate the total number of visits per provider per month
    val totalVisitsPerProviderPerMonth = joinedDF
      .groupBy("provider_id", "month")
      .agg(count("visit_id").alias("total_visits"))
    
    totalVisitsPerProviderPerMonth.write.json("output/total_visits_per_provider_per_month")

    // for unit testing, I give a return value.
    // For production code this would be done in 2 separate functions instead of the tuple return
    totalVisitsPerProvider -> totalVisitsPerProviderPerMonth

  }
  def main(args: Array[String]): Unit = {
    val providersDF = spark.read.option("header", "true").option("delimiter", "|")
      .csv("data/providers.csv")

    // Load the visits data, rename columns to snake_case, and add a month column
    val visitsDF = spark.read.option("header", "true")
      .csv("data/visits.csv")
      .withColumnRenamed("_c0", "visit_id")
      .withColumnRenamed("_c1", "provider_id")
      .withColumnRenamed("_c2", "date_of_service")
      .withColumn("month", month(to_date($"date_of_service", "yyyy-MM-dd")))

    ProviderRoster.process(providersDF, visitsDF)
  }
}
