package NDD_mapreduce

import NDD_mapreduce.io.{RowWriter, SpotifyRowReader}

/**
 * Execute a MapReduce job, on the spotify CSV file
 * @param mrf map and reduce functions
 * @param writer an object able to serialize the result of this MapReduce sequence.
 * @author Anne-Cecile Caron
 */
abstract class MainProcess (mrf : MapReduceFunctions, writer : RowWriter){

  /**
   * Execute a MapReduce job, on the spotify CSV file
   * @param args command line arguments. The first argument is the file path to the spotify CSV file. The second is optional and is the file path to the output CSV file
   */
  def main(args: Array[String]): Unit = {
    val spotify = SpotifyRowReader.readCSV(args(0), ",")
    val results = SimpleMapReduce.entireProcess(spotify, mrf)
    // print results sorted by key :
    val sortedResults = results.toSeq.sortWith((cp1, cp2) => (cp1._1 < cp2._1))
    for ((k, v) <- sortedResults) {
      println(k + " -> " + v.toString)
    }
    // output in CSV file
    if (args.length > 1)
         writer.writeCSV((for ((k, v) <- sortedResults) yield v).iterator, args(1), ",")
  }
}
