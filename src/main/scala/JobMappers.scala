import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

class JobMappers {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getValues(string_XML: String, value: String) : List[String] = {
    logger.info("Extracting the required values from the XML file")
    val parent = XML.loadString(string_XML)
    val elementList = (parent \\ value).map(el => el.text.toLowerCase.trim).toList
    elementList
  }
}

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

    val authors = new JobMappers().getValues(string_XML,"author") //extract list of authors from the XML string
    for(author <- authors) context.write(new Text(author), new io.IntWritable(1))
    logger.info("PublicationCountMapper execution completed")
  }
}

