/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.zhuohuawu.examples.sparktwo.sparkour

import org.apache.log4j.{Level, Logger}

import scala.util.Random
import org.apache.spark.sql.SparkSession

/**
 * Performs a variety of RDD manipulations to show off the data structure.
 */
object SWorkingRDDs {
	val logger = Logger.getLogger(SWorkingDataFrames.getClass)

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.INFO)
		Logger.getLogger(SWorkingDataFrames.getClass).setLevel(Level.INFO)

		val spark = SparkSession.builder.appName("SWorkingRDDs").getOrCreate()

		// Create an array of 1000 random numbers between 0 and 50.
		val numbers = Seq.fill(1000)(Random.nextInt(50))
	
		// Create an RDD from the numbers array
		val numbersListRdd = spark.sparkContext.parallelize(numbers)

		// Create an RDD from a similar array on the local filesystem
		val numberFilePath = "src/main/resources/sparkour/working-rdds/random_numbers.txt"
		val numbersFileRdd = spark.sparkContext.textFile(numberFilePath)

		// 1000 Chicago residents: How many books do you own?
		val chicagoRdd = numbersListRdd

		// 1000 Houston residents: How many books do you own?
		// Must convert from string data to ints first
		val houstonRdd = numbersFileRdd.flatMap(x => x.split(' '))
										.map(x => x.toInt)

		// How many have more than 30 in Chicago?
		val moreThanThirty = chicagoRdd.filter(x => x > 30).count()
		println(s"$moreThanThirty Chicago residents have more than 30 books.")

		// What's the most number of books in either city?
		val mostBooks = chicagoRdd.union(houstonRdd).reduce((x, y) => if (x > y) x else y)
		println(s"$mostBooks is the most number of books owned in either city.")

		// What's the total number of books in both cities?
		val totalBooks = chicagoRdd.union(houstonRdd).reduce((x, y) => x + y)
		println(s"$totalBooks books in both cities.")

		spark.sparkContext.stop()
	}
}
// scalastyle:on println
