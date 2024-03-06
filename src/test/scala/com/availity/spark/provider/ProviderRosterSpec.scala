package com.availity.spark.provider

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType, DateType}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.BeforeAndAfterEach

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  val spark: SparkSession =
    SparkSession.builder()
      .appName("Provider Visits Analysis Test")
      .master("local[*]")
      .getOrCreate()
  import spark.implicits._

  val providersDF = Seq(
    ("1", "Cardiology", "John", "A.", "Doe"),
    ("2", "Neurology", "Jane", "B.", "Smith")
  ).toDF("provider_id", "provider_specialty", "first_name", "middle_name", "last_name")

  val visitsDF = Seq(
    ("101", "1", "2022-01-01"),
    ("102", "1", "2022-01-02"),
    ("103", "2", "2022-01-01")
  ).toDF("visit_id", "provider_id", "date_of_service")
    .withColumn("date_of_service", to_date($"date_of_service", "yyyy-MM-dd"))
    .withColumn("month", month($"date_of_service"))

  describe("ProviderRoster.process") {

    it("calculates total visits per provider correctly") {
      val expectedTotalVisitsPerProvider = Seq(
        ("1", "Cardiology", "John", "A.", "Doe", 2),
        ("2", "Neurology", "Jane", "B.", "Smith", 1)
      ).toDF("provider_id", "provider_specialty", "first_name", "middle_name", "last_name", "total_visits")

      val (actualTotalVisitsPerProvider, _) = ProviderRoster.process(providersDF, visitsDF)

      assertSmallDataFrameEquality(actualTotalVisitsPerProvider, expectedTotalVisitsPerProvider, ignoreNullable = true, ignoreColumnNames = true)
    }

    it("calculates total visits per provider per month correctly") {
      val expectedTotalVisitsPerProviderPerMonth = Seq(
        ("1", 1, 2), // Provider 1 had 2 visits in January (month 1)
        ("2", 1, 1)  // Provider 2 had 1 visit in January
      ).toDF("provider_id", "month", "total_visits")

      val (_, actualTotalVisitsPerProviderPerMonth) = ProviderRoster.process(providersDF, visitsDF)

      assertSmallDataFrameEquality(actualTotalVisitsPerProviderPerMonth, expectedTotalVisitsPerProviderPerMonth, ignoreNullable = true, ignoreColumnNames = true)
    }
  }
}
