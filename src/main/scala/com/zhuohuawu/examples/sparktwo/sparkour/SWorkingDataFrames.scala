
// scalastyle:off println
package com.zhuohuawu.examples.sparktwo.sparkour

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.{Level, Logger}
/**
 * Performs a variety of DataFrames manipulations to show off the data structure.
 */
object SWorkingDataFrames {

	val logger = Logger.getLogger(SWorkingDataFrames.getClass)

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger(SWorkingDataFrames.getClass).setLevel(Level.ERROR)


		val spark = SparkSession.builder.appName("SWorkingDataFrames")
			//.master("local[4]")
			.getOrCreate()

		// Create a DataFrame based on the JSON results.
		val primaryInputPath = "src/main/resources/sparkour/working-dataframes/loudoun_d_primary_results_2016.json"
		val rawDF = spark.read.json(primaryInputPath)

		// Print the schema
		rawDF.printSchema()

		println("Who was on the ballet?")
		// Get all distinct candidate names from the DataFrame
		rawDF.select("candidate_name").distinct().show()
	
		println("What order were candidates on the ballot?")
		// Get the ballot order and discard the many duplicates (all VA ballots are the same)
		// Note the call to persist() -- we reuse this DataFrame later, so let's not execute it twice.
		val orderDF = rawDF.select(rawDF("candidate_name"), rawDF("candidate_ballot_order")) 
			.dropDuplicates().orderBy("candidate_ballot_order").persist()
		orderDF.show()
	
		println("What order were candidates on the ballot (in descriptive terms)?")
		// Load a reference table of friendly names for the ballot orders.

		val friendlyOrderInputPath = "src/main/resources/sparkour/working-dataframes/friendly_orders.json"
		val friendlyDF = spark.read.json(friendlyOrderInputPath)
		// Join the tables so the results show descriptive text
		val joinedDF = orderDF.join(friendlyDF, "candidate_ballot_order")
		// Hide the numeric column in the output.
		joinedDF.select(joinedDF("candidate_name"), joinedDF("friendly_name")).show()
	
		println("How many votes were cast?")
		// Orginal data is string-based. Create an integer version of the total votes column.
		val votesColumn = rawDF("total_votes").cast("int").alias("total_votes_int")
		// Get the integer-based votes column and sum all values together
		rawDF.select(votesColumn).groupBy().sum("total_votes_int").show()
	   
		println("How many votes did each candidate get?")
		// Get just the candidate names and votes.
		val candidateDF = rawDF.select(rawDF("candidate_name"), votesColumn)
		// Group by candidate name and sum votes. Assign an alias to the sum so we can order on that column.
		// Note the call to persist() -- we reuse this DataFrame later, so let's not execute it twice.
		val groupedDF = candidateDF.groupBy("candidate_name").agg(functions.sum("total_votes_int").alias("sum_column"))
		val summaryDF = groupedDF.orderBy(groupedDF("sum_column").desc).persist()
		summaryDF.show()
	
		println("Which polling station had the highest physical turnout?")
		// All physical precincts have a numeric code. Provisional/absentee precincts start with "##".
		// Spark's cast function converts these to "null".
		val precinctColumn = rawDF("precinct_code").cast("int").alias("precinct_code_int")
		// Get the precinct name, integer-based code, and integer-based votes, then filter on non-null codes.
		val pollingDF = rawDF.select(rawDF("precinct_name"), precinctColumn, votesColumn)
			.filter("precinct_code_int is not null")
		// Group by precinct name and sum votes. Assign an alias to the sum so we can order on that column.
		// Then, show the max row.
		val groupedPollDF = pollingDF.groupBy("precinct_name").agg(functions.sum("total_votes_int").alias("sum_column"))
		groupedPollDF.orderBy(groupedPollDF("sum_column").desc).limit(1).show()
	
		println("Saving overall candidate summary as a new JSON dataset.")
		summaryDF.write.mode("overwrite").json("target/json")

		spark.stop()
	}
}
// scalastyle:on println
