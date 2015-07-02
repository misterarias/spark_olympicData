package org.utad

import org.apache.spark.SparkContext._

/**
 * Ejercicio 1
 * Crea un proceso en Spark que agrupe por edad el numero de medallas conseguidas por los participantes de las olimpiadas.
 */
object OlympicMedalsAgeApp extends App {
  // Parse input data
  val olympicDataRdd = OlympicsHelper.readOlympicData()

  // Get a ranking according to specifications
  val medalsPerAgeRDD = OlympicsHelper.getRanking(olympicDataRdd.map(x => (x.getAge.toString, x)).groupByKey(2));

  // Format result for output, ordering by age
  medalsPerAgeRDD.collect.sorted(new Ordering[(Double, String)] {
    override def compare(x: (Double, String), y: (Double, String)): Int = y._2.toInt.compare(x._2.toInt)
  }).foreach(x=>printf("Edad %s:\t%d medallas\n", x._2, x._1.toInt))

  sys.exit(0)
}
