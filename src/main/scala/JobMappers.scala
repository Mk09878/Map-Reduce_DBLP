import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import java.util

import scala.collection.mutable
import scala.xml.XML

class JobMappers {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Returns the list of elements from the XML file based on the specified 'value' tag.
   * @param string_XML: XML format
   * @param value: Tag name
   * @return: List of required elements
   */
  def getValues(string_XML: String, value: String) : List[String] = {
    logger.info("Extracting the required values from the XML file")
    val parent = XML.loadString(string_XML)
    val elementList = (parent \\ value).map(el => el.text.toLowerCase.trim).toList
    elementList
  }
}

/**
 * The input to the mapper is the entire dataset from which it extracts the author list, and the venue for each publication.
 * It outputs the venue as key and value as the string conversion of the author list.
 */
class TopTenAuthorMapper extends Mapper[LongWritable, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val conf: Config = ConfigFactory.load("Config_File")
  val configuration = new Configuration()
  val dtd: URI = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Starting TopTenAuthorMapper")
    val string_XML =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtd">
              <dblp>""" + value.toString + "</dblp>"

    val authors = new JobMappers().getValues(string_XML,"author")
    if(authors.size <= 0) return

    val journal = new JobMappers().getValues(string_XML, "journal")
    if(journal.nonEmpty){
      context.write(new Text(journal.head), new Text(authors.mkString(";")))
    }

    val booktitle = new JobMappers().getValues(string_XML, "booktitle")
    if(booktitle.nonEmpty){
      context.write(new Text(booktitle.head), new Text(authors.mkString(";")))
    }

    val publisher = new JobMappers().getValues(string_XML, "publisher")
    if(publisher.nonEmpty){
      context.write(new Text(publisher.head), new Text(authors.mkString(";")))
    }
  }
}

/**
 * Basic mapper I had used to just count publications of each author
 */
class PublicationCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val conf: Config = ConfigFactory.load("Config_File")
  val configuration = new Configuration()
  val dtd: URI = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Starting PublicationCountMapper")
    val string_XML =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtd">
              <dblp>""" + value.toString + "</dblp>"

    val authors = new JobMappers().getValues(string_XML,"author")
    for(author <- authors) context.write(new Text(author), new io.IntWritable(1))
    logger.info("PublicationCountMapper execution completed")
  }
}

/**
 * The input to the mapper is the entire dataset from which it extracts the author list.
 * Outputs the author name as key and number of co-authors for that publication.
 */
class CoAuthorCountMapper extends Mapper[LongWritable, Text, Text, IntWritable]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val conf: Config = ConfigFactory.load("Config_File")
  val configuration = new Configuration()
  val dtd: URI = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Starting CoAuthorCountMapper")
    val string_XML =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtd">
              <dblp>""" + value.toString + "</dblp>"

    val authors = new JobMappers().getValues(string_XML,"author")
    for(author <- authors) context.write(new Text(author), new io.IntWritable(authors.size - 1))
    logger.info("CoAuthorCountMapper execution completed")
  }
}

/**
 * This mapper inverts the key and values which are then sorted by the DescendingComparator class in descending order
 */
class SortingMapper extends Mapper[LongWritable, Text, IntWritable, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
    logger.info("Starting sorting mapper")
    val tokens = value.toString.split(",")
    val author = tokens(0)
    val total_no_of_co_authors = tokens(1).toInt
    context.write(new IntWritable(total_no_of_co_authors), new Text(author))
    logger.info("Sorting mapper execution completed")
  }
}

/**
 * The input to the mapper is the entire dataset from which it extracts the author list for each publication, checks if there are no co-authors.
 * Outputs the author name as key, and a number 1 for the publication.
 */
class NoCoAuthorPublicationCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val conf: Config = ConfigFactory.load("Config_File")
  val configuration = new Configuration()
  val dtd: URI = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Starting NoCoAuthorPublicationCountMapper")
    val string_XML =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtd">
              <dblp>""" + value.toString + "</dblp>"

    val authors = new JobMappers().getValues(string_XML,"author")
    if(authors.size == 1){
      context.write(new Text(authors.head), new io.IntWritable(1))
    }
    logger.info("PublicationCountMapper execution completed")
  }
}

/**
 * The input to the mapper is the entire dataset from which it extracts the author list, publication title and the venue for each publication.
 * Checks if there are no co-authors and outputs the venue as key, and the title as value.
 */
class VenuePublicationOneAuthorMapper extends Mapper[LongWritable, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val conf: Config = ConfigFactory.load("Config_File")
  val configuration = new Configuration()
  val dtd: URI = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Starting PublicationCountMapper")
    val string_XML =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtd">
              <dblp>""" + value.toString + "</dblp>"


    val title = new JobMappers().getValues(string_XML, "title")
    val authors = new JobMappers().getValues(string_XML,"author")

    val journal = new JobMappers().getValues(string_XML, "journal")
    if(journal.nonEmpty){
      if(authors.size == 1){
        context.write(new Text(journal.head), new Text(title.head))
      }
    }

    val booktitle = new JobMappers().getValues(string_XML, "booktitle")
    if(booktitle.nonEmpty){
      if(authors.size == 1){
        context.write(new Text(booktitle.head), new Text(title.head))
      }
    }

    val publisher = new JobMappers().getValues(string_XML, "publisher")
    if(publisher.nonEmpty){
      if(authors.size == 1){
        context.write(new Text(publisher.head), new Text(title.head))
      }
    }

  }

}

/**
 * The input to the mapper is the entire dataset from which it extracts the author list, and the year for each publication.
 * The mapper creates a hashmap with key as the author name and value as a hashset of year (to ensure unique years for each author).
 * The mapper then outputs the key as author and the years as a string.
 */
class NYearsWithoutInterruptMapper extends Mapper[LongWritable, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val conf: Config = ConfigFactory.load("Config_File")
  val configuration = new Configuration()
  val dtd: URI = getClass.getClassLoader.getResource("dblp.dtd").toURI
  var hmap: mutable.HashMap[String, mutable.HashSet[Int]] = _

  override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    hmap = new mutable.HashMap[String, mutable.HashSet[Int]]
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Starting CoAuthorCountMapper")
    val string_XML =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtd">
              <dblp>""" + value.toString + "</dblp>"

    val authors = new JobMappers().getValues(string_XML,"author")
    val year = new JobMappers().getValues(string_XML,"year")
    if(year.size <= 0)
      return

    authors.foreach(author => {
      if(hmap.contains(author)){
        val year_set = hmap(author)
        year_set.add(year.head.toInt)
        hmap.put(author, year_set)
      }
      else{
        val year_set = new mutable.HashSet[Int]()
        year_set.add(year.head.toInt)
        hmap.put(author, year_set)
      }
    })
  }

  override def cleanup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    hmap.foreach(keyVal =>
      context.write(new Text(keyVal._1), new Text(keyVal._2.mkString(";")))
    )
  }

}

/**
 * The input to the mapper is the entire dataset from which it extracts the author list, the publication title and the venue for each publication.
 * It outputs the venue as key and value as the publication title concatenated with the number of authors.
 */
class HighestNumberofAuthorVenueMapper extends Mapper[LongWritable, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val conf: Config = ConfigFactory.load("Config_File")
  val configuration = new Configuration()
  val dtd: URI = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Starting HighestNumberofAuthorVenueMapper")
    val string_XML =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtd">
              <dblp>""" + value.toString + "</dblp>"

    val title = new JobMappers().getValues(string_XML, "title")
    //val title = titleog.head.split(";")(0)
    val authors = new JobMappers().getValues(string_XML,"author")
    if(authors.size <= 0) return

    val journal = new JobMappers().getValues(string_XML, "journal")
    if(journal.nonEmpty){
      context.write(new Text(journal.head), new Text("thead:" + title.head + ":>" + "AuthCnt:" + authors.size))
    }

    val booktitle = new JobMappers().getValues(string_XML, "booktitle")
    if(booktitle.nonEmpty){
      context.write(new Text(booktitle.head), new Text("thead:" + title.head + ":>" + "AuthCnt:" + authors.size))
    }

    val publisher = new JobMappers().getValues(string_XML, "publisher")
    if(publisher.nonEmpty){
      context.write(new Text(publisher.head), new Text("thead:" + title.head + ":>" + "AuthCnt:" + authors.size))
    }

  }
}