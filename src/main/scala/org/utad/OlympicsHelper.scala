package org.utad

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * Created by juanito on 9/02/15.
 */
object OlympicsHelper {

  var sc:SparkContext = null
  def getContext(): SparkContext = {
    if (sc == null) {
      sc = new SparkContext("local", "Ejercicio1z", "$SPARK_HOME");
    }
    return sc
  }

  def getInt(s: String): Int = s.length match {
    case 0 => 0
    case _ => Integer.parseInt(s)
  }

  /**
   * Read data from a headerless CSV file into an RDD of OlympicMedalRecords
   * @return RDD[OlympicMedalRecords]
   */
  def readOlympicData(): RDD[OlympicMedalRecords] = {
    val sc = getContext()
    val dataFile = "OlympicAthletes.csv"
    val olympicDataRdd = sc.textFile(dataFile, 2).cache()

    // Read CSV into an RDD[OlympicMedalRecords]
    //var firstRead = false
    val recordsRDD = olympicDataRdd.map(line => {
      val info = line.split(",")

      // Athlete,Age,Country,Year,Closing Ceremony Date,Sport,Gold Medals,Silver Medals,Bronze Medals,Total Medals
      val name = info(0) // name
      val age = getInt(info(1)) // age
      val country = info(2) // country
      val game = getInt(info(3)) // olympicGame
      // jump Closing Ceremony Data!!
      val sport = info(5) // sport
      val gMedal = getInt(info(6)) // gold Medals
      val sMedal = getInt(info(7)) // silver Medals
      val bMedal = getInt(info(8)) // bronze Medals

      new OlympicMedalRecords(name, age, country, game, sport, gMedal, sMedal, bMedal)
    })

    return recordsRDD;
  }

  /**
   * Return a ranking of an Athletes data RDD, based on certain criteria for medal value.
   *
   * @param data  Pairs of a certain key and an OlympicMedalRecords entry
   * @param weights Medal weights' values, corresponding to (gold, silver, bronze)
   * @return
   */
  def getRanking(
                  data: RDD[(String, Iterable[OlympicMedalRecords])],
                  weights: (Double, Double, Double) = (1.0, 1.0, 1.0)): RDD[(Double, String)] =
    data.map { x => {
      var points = 0.0
      x._2.foreach(x => points +=
        weights._1 * x.getGoldMedals +
          weights._2 * x.getSilverMedals +
          weights._3 * x.getBronzeMedals
      )

      (points, x._1)
    }}
}
