import java.lang

import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.mapreduce.ReduceContext

import scala.jdk.CollectionConverters._

class JobReducers extends Reducer[Text, IntWritable, Text, IntWritable]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /*override def run(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    setup(context)
    var count = 0
    try while ( {
      context.nextKey && count < 5
    }) {
      count += 1
      reduce(context.getCurrentKey, context.getValues, context)
      // If a back up store is used, reset it
      val iter = context.getValues.iterator
      if (iter.isInstanceOf[ReduceContext.ValueIterator[_]]) iter.asInstanceOf[ReduceContext.ValueIterator[Nothing]].resetBackupStore()
    }
    finally cleanup(context)
  }*/

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.info("Starting reducer")
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    context.write(key, new io.IntWritable(sum))
    logger.info("Reducer execution completed")
  }

}
