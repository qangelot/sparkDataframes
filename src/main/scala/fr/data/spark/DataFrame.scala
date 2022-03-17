package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("job-1")
      .master("local[2]")
      .getOrCreate()

    // dataframe : pas besoin de case class
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("src/main/resources/codesPostaux.csv")

    // Show schema
    println("Dataframe Schema: ")
    df.printSchema()

    // Number of columns
    println("Number of columns: " + df.columns.size)

    // Distinct cities
    println("Distinct cities: " + df.select("Code_commune_INSEE").distinct().count())

    // Number of cities with ligne_5 not null
    val ligne5NotNull = df.na.drop("any", Seq("Ligne_5"))
    println("Number of cities with ligne_5: " + ligne5NotNull.count())

    // Add columns with dept code
    val dfEnhanced = df.withColumn("Code_departement", substring(col("Code_commune_INSEE"), 1, 2))
    println("Add columns with dept code: ")
    dfEnhanced.show()
    
    // Save to csv
    dfEnhanced.select("Code_commune_INSEE", "Nom_commune", "Code_postal", "Code_departement")
      .sort("Code_postal")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/main/resources/commune_et_departement.csv")

    // Cities in Aisne
    val AisneDF = dfEnhanced.filter(dfEnhanced("Code_departement")==="02")
    println("Cities in Aisne: ")
    AisneDF.show()

    // Department with the most Cities
    println("Department with the most Cities: ")
    val depWithMostCities = dfEnhanced.groupBy("Code_departement").count().sort(desc("count")).select("Code_departement").show(1)

  }



}
