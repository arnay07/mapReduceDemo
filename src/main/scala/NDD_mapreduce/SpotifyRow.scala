package NDD_mapreduce

import java.sql.Date

/** A line of spotify statistics : ranking position on a specific day for a song
 * The data is read-only
 *
 * @param pos : ranking position
 * @param track : title of song
 * @param artist : name of artist, musician or group
 * @param nbStreams : number of streams
 * @param streamingDate : date of this ranking position
 * @author Anne-Cecile Caron
 */
class SpotifyRow(val pos : Int,
                 val track: String,
                 val artist : String,
                 val nbStreams : Int,
                 val streamingDate : Date) extends Row {
  override def toString: String = "("+pos+", "+track+", "+artist+", "+nbStreams+", "+streamingDate+")"
}