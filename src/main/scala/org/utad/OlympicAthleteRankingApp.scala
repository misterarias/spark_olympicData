package org.utad

import org.apache.spark.SparkContext._

/**
 * Ejercicio 2
 * Dando puntos a cada una de las medallas (3 puntos oro, 2 puntos plata y 1 bronce) haz un ranking con los mejores deportistas.
 */
object OlympicAthleteRankingApp extends App {
  // Parse input data
  val olympicDataRdd = OlympicsHelper.readOlympicData()

  // Get a ranking according to specifications
  val medalRanking = OlympicsHelper.getRanking(olympicDataRdd.map(x => (x.getName, x)).groupByKey(2), (3.0, 2.0, 1.0));

  // Format result for output
  medalRanking.sortByKey(false).foreach(x=>printf("'%s':\t%d puntos\n",x._2, x._1.toInt))

  sys.exit(0)
}
