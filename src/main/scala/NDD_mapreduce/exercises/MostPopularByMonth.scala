package NDD_mapreduce.exercises

import NDD_mapreduce.{MapReduceFunctions, Row, SpotifyRow, StringIntCouple}

object MostPopularByMonth extends MapReduceFunctions{

/**
   * @param elt one input row from the StringIntCouple sent from the first job
   * @return one result per input row, with unique key, the streaming month
   */
  def map(elt: Row): List[(String, Row)] = {
    val couple: (String,Row) = (elt.asInstanceOf[StringIntCouple].str.substring(0,7)
      , new StringIntCouple(elt.asInstanceOf[StringIntCouple].str.substring(8), elt.asInstanceOf[StringIntCouple].num))
    List[(String, Row)](couple)
  }

  /**
   * @param key output key sent by the mapper the streaming date
   * @param results the list of the max, min and average of all the streams for this date
   * @return the element inside the list, with its key
   */
  def reduce(key: String, results: List[Row]) : (String, Row) = {
    var max = results.head.asInstanceOf[StringIntCouple].num
    var maxRow = results.head
    for (result <- results) {
      if (result.asInstanceOf[StringIntCouple].num > max) {
        max = result.asInstanceOf[StringIntCouple].num
        maxRow = result
      }
    }
    (key, maxRow)
  }
}
