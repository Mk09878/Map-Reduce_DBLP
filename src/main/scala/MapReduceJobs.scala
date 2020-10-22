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


    /*logger.info("Starting first job")
    val job1 = Job.getInstance(configuration, "Number of sub-authors for each author")
    val output_second = new Path(args(1) + "Number of sub-authors for each author")
    job1.setJarByClass(this.getClass)
    job1.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])

    job1.setMapperClass(classOf[CoAuthorCountMapper])
    job1.setCombinerClass(classOf[JobReducers])
    job1.setReducerClass(classOf[JobReducers])

    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[IntWritable])

    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, output_second)
    job1.waitForCompletion(true)

    logger.info("Starting second job")
    val job2 = Job.getInstance(configuration, "Top 100 authors with most co-authors")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[SortingMapper])
    job2.setReducerClass(classOf[SortingReducer])

    job2.setMapOutputKeyClass(classOf[IntWritable])
    job2.setMapOutputValueClass(classOf[Text])

    job2.setSortComparatorClass(classOf[DescendingComparator])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.setInputPaths(job2, output_second)
    FileOutputFormat.setOutputPath(job2, new Path(args(1) + "Top100coauthors"))
    job2.waitForCompletion(true)

    logger.info("Starting third job")
    val job3 = Job.getInstance(configuration, "Number of publications for each author with no co-author")
    val output_fourth = new Path(args(1) + "Number of publications for each author with no co-author")
    job3.setJarByClass(this.getClass)
    job3.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])

    job3.setMapperClass(classOf[NoCoAuthorPublicationCountMapper])
    job3.setCombinerClass(classOf[JobReducers])
    job3.setReducerClass(classOf[JobReducers])

    job3.setMapOutputKeyClass(classOf[Text])
    job3.setMapOutputValueClass(classOf[IntWritable])

    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job3, new Path(args(0)))
    FileOutputFormat.setOutputPath(job3, output_fourth)
    job3.waitForCompletion(true)

    logger.info("Starting fourth job")
    val job4 = Job.getInstance(configuration, "Top 100 authors with no co-author")
    job4.setJarByClass(this.getClass)

    job4.setMapperClass(classOf[SortingMapper])
    job4.setReducerClass(classOf[SortingReducer])

    job4.setMapOutputKeyClass(classOf[IntWritable])
    job4.setMapOutputValueClass(classOf[Text])

    job4.setSortComparatorClass(classOf[DescendingComparator])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.setInputPaths(job4, output_fourth)
    FileOutputFormat.setOutputPath(job4, new Path(args(1) + "Top100nocoauthor"))
    job4.waitForCompletion(true)

    logger.info("Starting fifth job")
    val job5 = Job.getInstance(configuration, "VenuePublicationOneAuthor")
    job5.setJarByClass(this.getClass)
    job5.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])

    job5.setMapperClass(classOf[VenuePublicationOneAuthorMapper])
    job5.setCombinerClass(classOf[VenuePublicationOneAuthorReducer])
    job5.setReducerClass(classOf[VenuePublicationOneAuthorReducer])

    job5.setMapOutputKeyClass(classOf[Text])
    job5.setMapOutputValueClass(classOf[Text])

    job5.setOutputKeyClass(classOf[Text])
    job5.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job5, new Path(args(0)))
    FileOutputFormat.setOutputPath(job5, new Path(args(1) + "VenuePublicationOneAuthor"))
    job5.waitForCompletion(true)

    logger.info("Starting sixth job")
    val job6 = Job.getInstance(configuration, "NYearsWithoutInterrupt")
    job6.setJarByClass(this.getClass)
    job6.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])

    job6.setMapperClass(classOf[NYearsWithoutInterruptMapper])
    job6.setCombinerClass(classOf[NYearsWithoutInterruptReducer])
    job6.setReducerClass(classOf[NYearsWithoutInterruptReducer])

    job6.setMapOutputKeyClass(classOf[Text])
    job6.setMapOutputValueClass(classOf[Text])

    job6.setOutputKeyClass(classOf[Text])
    job6.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job6, new Path(args(0)))
    FileOutputFormat.setOutputPath(job6, new Path(args(1) + "NYearsWithoutInterrupt"))
    job6.waitForCompletion(true)

    logger.info("Starting seventh job")
    val job7 = Job.getInstance(configuration, "TopTenAuthor")
    job7.setJarByClass(this.getClass)
    job7.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job7.setMapperClass(classOf[TopTenAuthorMapper])
    job7.setCombinerClass(classOf[TopTenAuthorReducer])
    job7.setReducerClass(classOf[TopTenAuthorReducer])
    job7.setMapOutputKeyClass(classOf[Text])
    job7.setMapOutputValueClass(classOf[Text])

    job7.setOutputKeyClass(classOf[Text])
    job7.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job7, new Path(args(0)))
    FileOutputFormat.setOutputPath(job7, new Path(args(1) + "TopTenAuthor"))
    job7.waitForCompletion(true)*/

    logger.info("Starting eighth job")
    val job8 = Job.getInstance(configuration, "HighestNumberofAuthorVenue")
    job8.setJarByClass(this.getClass)
    job8.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job8.setMapperClass(classOf[HighestNumberofAuthorVenueMapper])
    job8.setCombinerClass(classOf[HighestNumberofAuthorVenueReducer])
    job8.setReducerClass(classOf[HighestNumberofAuthorVenueReducer])
    job8.setMapOutputKeyClass(classOf[Text])
    job8.setMapOutputValueClass(classOf[Text])

    job8.setOutputKeyClass(classOf[Text])
    job8.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job8, new Path(args(0)))
    FileOutputFormat.setOutputPath(job8, new Path(args(1) + "HighestNumberofAuthorVenue"))
    job8.waitForCompletion(true)

  }

}
