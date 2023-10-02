package NDD_mapreduce

import NDD_mapreduce.io.{SpotifyRowReader, StringIntCoupleWriter}

import scala.collection.mutable

/**
 * Simple implementation of MapReduce on Spotify data.
 * Three phases : map, shuffle, reduce
 * @author Anne-Cecile Caron
 */
object SimpleMapReduce {

  /**
   * map phase. Filter the data such that an input row can yield 0, 1 or more results
   * @param allData : contains the input rows
   * @param mapFunction : the map function applied to each row
   * @return : (key,value) pairs, results of all input rows
   */
  def mapPhase(allData : Iterator[Row], mapFunction: Row => List[(String, Row)] ): Iterator[(String, Row)] = {
    allData.flatMap(mapFunction)
  }

  /**
   * shuffle phase.
   * redistribute data based on the output keys (i.e keys produced by the map function),
   * such that all data belonging to one key is given to the same reducer
   * @param mapOutput : sequence of pairs (key,value), result of the map phase
   * @return : pairs (key, list of values). Each key is unique
   */
  def shufflePhase(mapOutput : Iterator[(String, Row)]) : mutable.Map[String, List[Row]] = {
    var myMap = new mutable.HashMap[String, List[Row]]
    for ((s,t) <- mapOutput)
      myMap.get(s) match {
        case Some(l) => myMap update (s, t+:l )
        case None => myMap put (s, List[Row](t))
      }
    myMap
  }

  /**
   * reduce phase
   * process each group of output data coming from the map phase, per key,
   * @param reduceLists : pairs (key, list of values). Each key is unique
   * @param reduceFunction : reduce function applied to the list of values associated to 1 key
   * @return : pairs (key, value). Each key is unique
   */
  def reducePhase(reduceLists : mutable.Map[String, List[Row]], reduceFunction: (String, List[Row]) => (String, Row)):  mutable.Map[String, Row]= {
    var myMap = new mutable.HashMap[String, Row]
    for ((k,l) <- reduceLists) myMap put (k, reduceFunction(k,l)._2) // f(k,l)._2 means the second element of the tuple returned by f(k,l)
    myMap
  }

  /**
   * Three phases : map, shuffle, reduce
   * @param allData : the input of map phase
   * @param mrf : map and reduce function to apply during the process
   * @return : the final result, i.e.pairs (key,value) resulting from the reduce phase, keys are unique
   */
  def entireProcess(allData : Iterator[Row], mrf : MapReduceFunctions) : mutable.Map[String, Row] = {
    // map phase :
    val mapResult = mapPhase(allData, mrf.map)
    // shuffle phase
    val shuffleResult = shufflePhase(mapResult)
    // reduce phase
    reducePhase(shuffleResult, mrf.reduce)
  }

}
