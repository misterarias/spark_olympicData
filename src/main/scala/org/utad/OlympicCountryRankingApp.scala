package org.utad

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


/**
 * Ejercicio 3
 * Con la puntuación anterior crea un ranking con los tres mejores, en caso de que los haya, países para cada una de las disciplinas.
 */
object OlympicCountryRankingApp extends App {
  // Parse input data
  val olympicDataRdd = OlympicsHelper.readOlympicData()

  // Create an array of items grouped by discipline
  var rankingList = List[(String, Array[(Double, String)])]()
  val medalsPerDiscipline = olympicDataRdd.map(x => (x.getSport, x)).groupByKey(2).cache().collect()

  // For each sport, create a RDD to do the calculations
  val sc = OlympicsHelper.getContext()
  medalsPerDiscipline.map(y => {
    val sport = y._1
    val data = y._2.toArray.map(x => (x.getCountry, x))
    val rdd = sc.parallelize(data)

    // Get the top 3 medal-winning countries for each country in the main loop
    val medalRanking: RDD[(Double, String)] =
      OlympicsHelper.getRanking(rdd.groupByKey(2), (3.0, 2.0, 1.0));
    val topSports: Array[(Double, String)] =
      medalRanking.sortByKey(false).take(3)

    val item: (String, Array[(Double, String)]) = (sport, topSports)
    rankingList :::= List(item)
  })

  // Once data is loaded, dump it in STDOUT
  rankingList.foreach(sport => {
    printf("\nSport: %s\n", sport._1)
    sport._2.foreach(medals => {
      printf("\t%s (%d)\n", medals._2, medals._1.toInt)
    })
  })

  sys.exit(0)
}
