import java.lang

import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._

class JobReducers extends Reducer[Text, IntWritable, Text, IntWritable]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.info("Starting reducer")
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    context.write(key, new io.IntWritable(sum))
    logger.info("Reducer execution completed")
  }

}
