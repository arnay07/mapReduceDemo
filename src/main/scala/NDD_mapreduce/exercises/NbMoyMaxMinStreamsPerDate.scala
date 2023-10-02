package NDD_mapreduce.exercises

import NDD_mapreduce.{MapReduceFunctions, Row, StringIntCouple}


object NbMoyMaxMinStreamsPerDate extends MapReduceFunctions{

  /**
   * @param elt one input row from the Spotify dataset
   * @return one result per input row, with unique key, the streaming date
   */
  def map(elt: Row): List[(String, Row)] = NbStreamsPerDate.map(elt)

  /**
   * @param key output key sent by the mapper the streaming date
   * @param results the list of the max, min and average of all the streams for this date
   * @return the element inside the list, with its key
   */
  def reduce(key: String, results: List[Row]) : (String, Row) = {
    var cpt = 0.0
    var max = results.head.asInstanceOf[StringIntCouple].num
    var min = results.head.asInstanceOf[StringIntCouple].num
    var moyen = 0.0
    for (result <- results) {
      if (result.asInstanceOf[StringIntCouple].num < min) min = result.asInstanceOf[StringIntCouple].num
      if (result.asInstanceOf[StringIntCouple].num > max) max = result.asInstanceOf[StringIntCouple].num
      cpt += result.asInstanceOf[StringIntCouple].num
    }
    moyen = cpt/results.length
    (key, new RowStringIntX3(key, moyen, max, min))
  }
}