package NDD_mapreduce.exercises

import NDD_mapreduce.{MapReduceFunctions, Row, SpotifyRow, StringIntCouple}


object NbStreamsPerDatePopular extends MapReduceFunctions {

  /**
   * @param elt one input row from the Spotify dataset
   * @return one result per input row, with unique key, the ranking position and the streaming date
   */
  def map(elt: Row): List[(String, Row)] = {
    if (elt.asInstanceOf[SpotifyRow].pos > 3) return List[(String, Row)]()
    val couple: (String,Row) =
      (elt.asInstanceOf[SpotifyRow].streamingDate + " - " + elt.asInstanceOf[SpotifyRow].pos, new StringIntCouple(elt.toString, elt.asInstanceOf[SpotifyRow].nbStreams))
    List[(String, Row)](couple)
  }

  /**
   * @param key output key sent by the mapper the streaming date
   * @param results the list of all the streams for this date and ranking position
   * @return the element inside the list, with its key
   */
  def reduce(key: String, results: List[Row]) : (String, Row) = NbStreamsPerDate.reduce(key, results)
}