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
    val input = new Path(args(0))
    val output = new Path(args(1))
    val configuration = new Configuration()
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
    FileInputFormat.addInputPath(job1, input)
    FileOutputFormat.setOutputPath(job1, output)
    System.exit(if (job1.waitForCompletion(true)) 0 else 1)
  }

}
