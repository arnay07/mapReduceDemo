package NDD_mapreduce.io

import NDD_mapreduce.StringIntCouple

/** [[RowReader]] for [[StringIntCouple]] objects.
 * Deserialization of a serialization produced by [[StringIntCoupleWriter]]
 */
object StringIntCoupleReader extends RowReader {
  /**
   *
   * @param pathToFile the path to the input CSV file, for instance '/home/toto/myrows.csv
   * @param delimiter
   *  @return a Row iterator, each Row object corresponds to a CSV row
   */
  def readCSV(pathToFile: String, delimiter: String) = {
    val source = scala.io.Source.fromFile(pathToFile)
    val data = source.getLines.drop(1)

    for (line <- data) yield { // for ... yield is a for expression that return values, here SpotifyRow objects
      val t = line.split(delimiter)
      new StringIntCouple(t(0), Integer.parseInt(t(1)))
    }
  }

}
