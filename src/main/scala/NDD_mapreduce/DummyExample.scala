package NDD_mapreduce

/**
 * These map and reduce functions implements (quasi-)identity
 * For instance, an input row (39, Naha, PNL, 46660, 2017-02-28)
 * gives an output result ("39-2017-02-28" , StringIntCouple( "(39, Naha, PNL, 46660, 2017-02-28)", 1) )
 * @author Anne-Cecile Caron
 */
object DummyExample extends MapReduceFunctions {

  /**
   * @param elt one input row from the Spotify dataset
   * @return one result per input row, with unique key, concatenation of position and date fields
   */
  def map(elt: Row): List[(String, Row)] = {
    val couple: (String,Row)
    = (elt.asInstanceOf[SpotifyRow].pos + "-" + elt.asInstanceOf[SpotifyRow].streamingDate, new StringIntCouple(elt.toString, 1))
    List[(String, Row)](couple)
  }

  /**
   * @param key output key sent by the mapper, concatenation of position and date fields
   * @param results a list of size 1, since the key is a primary key for the Spotify dataset
   * @return the element inside the list, with its key
   */
  def reduce(key: String, results: List[Row]) : (String, Row) = {
    (key,results.head)
  }
}
