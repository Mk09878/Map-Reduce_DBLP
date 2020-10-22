import java.lang

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.mapreduce.ReduceContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
 * The reducer combines all the authors for each venue into a list.
 * The groupby and the sortBy function is applied on the list to get the top 10 frequent authors in the  authors_list on which map is used to get the author names.
 * Now, the reducer outputs the key as venue, and a string conversion of the top ten author list as value.
 * The output format is (venue,author1;author2;author3;....;author10)
 */
class TopTenAuthorReducer extends Reducer[Text, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.debug("In reducer class")

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    val author_list = new ListBuffer[String]
    values.forEach(x => {
      val temp = x.toString.split(";")
      temp.foreach(y => author_list += y)
    })

    if(author_list.nonEmpty) {
      val topTen = author_list.groupBy(identity).toList.sortBy(-_._2.size).take(10)
      val topTenAuthors = topTen.map { case (k, v) => s"$k" }
      if (topTen.nonEmpty)
        context.write(key, new Text(topTenAuthors.mkString(";")))
    }
  }
}

/**
 * Sums up the values and just outputs the key as input key and value as the sum.
 */
class JobReducers extends Reducer[Text, IntWritable, Text, IntWritable]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.info("Starting reducer")
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    context.write(key, new io.IntWritable(sum))
    logger.info("Reducer execution completed")
  }
}

/**
 * The reducer just inverts the keys and values to get the output in the format of (author -> number of publications) and only writes the top 100 authors.
 */
class SortingReducer extends Reducer[IntWritable, Text, Text, IntWritable]{
  var counter = 0
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Starting sorting reducer")
    values.forEach(value => {
      if(counter > 100)
        return
      counter += 1
      context.write(value, key)
    })
    logger.info("Sorting Reducer execution completed")
  }
}

/**
 * Concatenates the publication titles for each venue.
 * The delimiter used in this case is '|'.
 * The output format is (venue,title1 | title2 |)
 */
class VenuePublicationOneAuthorReducer extends Reducer[Text, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    logger.info("Starting VenuePublicationOneAuthorReducer")
    val sum = values.asScala.foldLeft("")((a, b) => a + b + " | ")
    context.write(key, new Text(sum))
  }

}

/**
 * The reducer creates a hashmap with the same features as the mapper. However, for each author it will become the years from each mapper.
 * Now, the reducer creates a treemap from the hashmap and just outputs the author with the consecutive N years.
 * The output format is (author,year1;year2;year3;......;yearN)
 */
class NYearsWithoutInterruptReducer extends Reducer[Text, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var hmap: mutable.HashMap[String, mutable.HashSet[Int]] = _
  val conf: Config = ConfigFactory.load("Config_File")

  override def setup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    hmap = new mutable.HashMap[String, mutable.HashSet[Int]]
  }

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    values.forEach( x => {
      val str_arr = x.toString.split(";")
      val set = new mutable.HashSet[Int]()
      str_arr.foreach(y => set.add(y.toInt))
      hmap.put(key.toString, set)
    })
  }

  override def cleanup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    hmap.foreach(keyVal => {
      var temp = -1
      var counter = 0
      val ss = collection.mutable.TreeSet(keyVal._2.toList: _*)
      ss.foreach(x => {
        if(temp == -1){
          temp = x
          counter += 1
        }
        else if(counter == conf.getInt("Years")) context.write(new Text(keyVal._1), new Text(keyVal._2.mkString(";")))
        else{
          if(x - temp == 1){
            counter += 1
          }
          else{
            counter = 0
          }
          temp = x
        }
      })
    })
  }
}

/**
 * The reducer creates a TreeMap with key as the number of authors for that publication and value as the publication titles.
 * The reducer then outputs the key as venue and the value as the highest entry(value of the entry) from the TreeMap.
 * The output format is (venue,publication1;publication2;....;publicationN)
 */
class HighestNumberofAuthorVenueReducer extends Reducer[Text, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    try {
      val tmap: mutable.TreeMap[Int, String] = new mutable.TreeMap[Int, String]()(implicitly[Ordering[Int]].reverse)

      val pattern = """thead:(.+):>AuthCnt:(\d+)""".r
      values.forEach(tuple => {
        val pattern(t, a) = tuple.toString
        if (tmap.contains(a.toInt))
          tmap.put(a.toInt, tmap(a.toInt) + ";" + t)
        else
          tmap.put(a.toInt, t)
      })
      context.write(key, new Text(tmap(tmap.firstKey)))
    }
    catch {
      case matchError: MatchError => logger.error(matchError.getMessage())
    }

  }

}