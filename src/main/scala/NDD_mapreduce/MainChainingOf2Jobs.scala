package NDD_mapreduce

import NDD_mapreduce.io.{RowWriter, SpotifyRowReader}

/** Execute a sequence (chain) of 2 MapReduce jobs, starting from the spotify CSV file
 * @param mrf1 map reduce functions of the first job
 * @param mrf2 map reduce functions of the second job
 * @param writer an object able to serialize the result of this MapReduce sequence.
 * @author Anne-Cecile Caron
 */
abstract class MainChainingOf2Jobs (mrf1 : MapReduceFunctions, mrf2 : MapReduceFunctions, writer : RowWriter){
  /**
   * Execute a sequence (chain) of 2 MapReduce jobs, starting from the spotify CSV file
   * @param args command line arguments. The first argument is the file path to the spotify CSV file. The second is optional and is the file path to the output CSV file
   */
  def main(args: Array[String]): Unit = {
    // first job
    val spotify = SpotifyRowReader.readCSV(args(0), ",")
    val resultsJob1 = SimpleMapReduce.entireProcess(spotify, mrf1)
    // second job
    val inputJob2 = for (couple <- resultsJob1) yield couple._2
    val resultsJob2 = SimpleMapReduce.entireProcess(inputJob2.iterator, mrf2)
    // print results sorted by key :
    val sortedResults = resultsJob2.toSeq.sortWith((cp1, cp2) => (cp1._1 < cp2._1))
    for ((k, v) <- sortedResults) {
      println(k + " -> " + v.toString)
    }

    // output in CSV file
    if (args.length > 1)
      writer.writeCSV((for ((k, v) <- sortedResults) yield v).iterator, args(1), ",")
  }
}
