package NDD_mapreduce.io

import NDD_mapreduce.SpotifyRow

import java.sql.Date

/** [[NDD_mapreduce.io.RowReader]] for spotify data.
 * Here, the CSV file comes from https://www.kaggle.com/datasets/edumucelli/spotifys-worldwide-daily-song-ranking
 *
 * Note that there are some lines with no track and no author :
 (11, , , 89528, 2017-07-20)
(34, , , 56812, 2017-07-20)
(39, , , 55701, 2017-07-20)
(122, , , 23290, 2017-07-20)
(15, , , 87212, 2017-07-21)
(28, , , 68460, 2017-07-21)
(37, , , 57977, 2017-07-21)
(171, , , 23825, 2017-07-21)
(199, , , 18868, 2017-07-22)
(192, , , 16888, 2017-07-23)
 * I have replaced by different labels ("unknown value 1" etc)
 */
object SpotifyRowReader extends RowReader {
  def readCSV(pathToFile: String, delimiter: String) = {
    val source = scala.io.Source.fromFile(pathToFile)
    val data = source.getLines.drop(1)
    var cpt_unknown_value = 1

    for (line <- data) yield { // for ... yield is a for expression that return values, here SpotifyRow objects
      val t = line.split(delimiter)
      // first elt : id
      val an_id = t(0).toInt
      // second elt : track
      var a_track = t(1)
      var j = 2
      // A track name with a ',' has been decomposed into several elements
      if (a_track.length != 0)
        if (a_track.charAt(0) == '"' && t.length > 5 && a_track.charAt(a_track.length - 1) != '"') {
          while (t(j).charAt(t(j).length - 1).compareTo('"') != 0) {
            a_track = a_track + ',' + t(j)
            j += 1
          }
          a_track = a_track + ',' + t(j)
          j += 1
        }
      // next elt : the artist
      // same pb, the name of an artist can own a ','
      var an_artist = t(j)
      j += 1
      if (an_artist.length != 0)
        if (an_artist.charAt(0) == '"' && t.length > 5 && an_artist.charAt(an_artist.length - 1) != '"') {
          while (t(j).charAt(t(j).length - 1).compareTo('"') != 0) {
            an_artist = an_artist + ',' + t(j)
            j += 1
          }
          a_track = a_track + ',' + t(j)
          j += 1
        }
      // next elt : number of streams
      val a_number = t(j).toInt
      j = j + 1
      // next elt : streaming date
      val a_date = Date.valueOf(t(j))
      if (a_track.length == 0) a_track = "unknown value "+cpt_unknown_value
      cpt_unknown_value += 1
      if (an_artist.length == 0) an_artist = "unknown value "+cpt_unknown_value
      cpt_unknown_value += 1
      new SpotifyRow(an_id, a_track, an_artist, a_number, a_date)
    }
  }
}
