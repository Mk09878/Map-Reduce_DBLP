import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.LoggerFactory

object MapReduceJobs {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val conf = ConfigFactory.load("Config_File")
    val configuration = new Configuration()
    configuration.set("mapred.textoutputformat.separator", ",")
    //loading start tags
    configuration.set("xmlinput.start", conf.getString("START_TAGS"))

    //loading end_tags
    configuration.set("xmlinput.end", conf.getString("END_TAGS"))

    configuration.set("io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization," +
        "org.apache.hadoop.io.serializer.WritableSerialization")

    logger.info("Starting first job")
    val job1 = Job.getInstance(configuration, "Publication Count")
    job1.setJarByClass(this.getClass)
    job1.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])

    job1.setMapperClass(classOf[PublicationCountMapper])
    job1.setCombinerClass(classOf[JobReducers])
    job1.setReducerClass(classOf[JobReducers])

    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, new Path(args(1) + "Publication Count"))
    job1.waitForCompletion(true)

    logger.info("Starting second job")
    val job2 = Job.getInstance(configuration, "Number of sub-authors for each author")
    job2.setJarByClass(this.getClass)
    job2.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job2.setMapperClass(classOf[CoAuthorCountMapper])
    job2.setCombinerClass(classOf[JobReducers])
    job2.setReducerClass(classOf[JobReducers])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job2, new Path(args(0)))
    FileOutputFormat.setOutputPath(job2, new Path(args(1) + "Number of sub-authors for each author"))
    job2.waitForCompletion(true)

    /*logger.info("Starting third job")
    val job3 = Job.getInstance(configuration, "Top 100 authors with most co-authors")
    job3.setJarByClass(this.getClass)
    job3.setMapperClass(classOf[SortingMapper])
    job3.setReducerClass(classOf[SortingReducer])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])*/

  }

}
