import org.apache.hadoop.io.{IntWritable, WritableComparable, WritableComparator}

class DescendingComparator extends WritableComparator(classOf[IntWritable], true) {

  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val key1 = a.asInstanceOf[IntWritable]
    val key2 = b.asInstanceOf[IntWritable]
    -1 * key1.compareTo(key2) // Sorts in descending order
  }
}