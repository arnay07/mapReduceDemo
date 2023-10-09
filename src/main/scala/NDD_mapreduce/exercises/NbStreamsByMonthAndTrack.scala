package NDD_mapreduce.exercises

import NDD_mapreduce.{MapReduceFunctions, Row, SpotifyRow, StringIntCouple}

object NbStreamsByMonthAndTrack extends MapReduceFunctions{

/**
   * @param elt one input row from the Spotify dataset
   * @return one result per input row, with unique key, the streaming month
   */
  def map(elt: Row): List[(String, Row)] = {
    val couple: (String,Row) = (elt.asInstanceOf[SpotifyRow].streamingDate.toString.substring(0,7) + " - " + elt.asInstanceOf[SpotifyRow].track , new StringIntCouple(elt.asInstanceOf[SpotifyRow].track, elt.asInstanceOf[SpotifyRow].nbStreams))
    List[(String, Row)](couple)
  }

  /**
   * @param key output key sent by the mapper the streaming date
   * @param results the list of the max, min and average of all the streams for this date
   * @return the element inside the list, with its key
   */
  def reduce(key: String, results: List[Row]) : (String, Row) = NbStreamsPerDate.reduce(key, results)
}
