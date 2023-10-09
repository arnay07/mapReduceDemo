package NDD_mapreduce.exercises

import NDD_mapreduce.MainChainingOf2Jobs
import NDD_mapreduce.io.StringIntCoupleWriter

object MainBestTrackByMonth extends MainChainingOf2Jobs(NbStreamsByMonthAndTrack , MostPopularByMonth, StringIntCoupleWriter){
}
