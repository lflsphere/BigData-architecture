import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala


object WordCount {

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val words = value.toString.split(" ")
      words.foreach(x => context.write(new Text(x), one))
    }
  }

  class SumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[SumReducer])
    job.setReducerClass(classOf[SumReducer])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }
}