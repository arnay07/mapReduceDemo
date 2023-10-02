package NDD_mapreduce.exercises

import NDD_mapreduce.{MapReduceFunctions, Row, SpotifyRow, StringIntCouple}


object NbStreamsPerDate extends MapReduceFunctions {

  /**
   * @param elt one input row from the Spotify dataset
   * @return one result per input row, with unique key, the streaming date
   */
  def map(elt: Row): List[(String, Row)] = {
    val couple: (String,Row) = (elt.asInstanceOf[SpotifyRow].streamingDate.toString, new StringIntCouple(elt.toString, elt.asInstanceOf[SpotifyRow].nbStreams))
    List[(String, Row)](couple)
  }

  /**
   * @param key output key sent by the mapper the streaming date
   * @param results the list of all the streams for this date
   * @return the element inside the list, with its key
   */
  def reduce(key: String, results: List[Row]) : (String, Row) = {
    var cpt = 0
    for (result <- results) {
      cpt += result.asInstanceOf[StringIntCouple].num
    }
    (key, new StringIntCouple(key, cpt))
  }
}
