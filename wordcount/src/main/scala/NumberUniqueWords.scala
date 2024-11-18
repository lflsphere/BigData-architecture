import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala


object NumberUniqueWords {

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val words = value.toString.map(_.toLower).replaceAll("[^A-Za-z0-9 ]", "").split(" ")
      words.foreach(x => context.write(new Text(x), one))
    }
  }

  class OneReducer extends Reducer[Text, IntWritable, IntWritable, IntWritable] {
    val one = new IntWritable(1)
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, IntWritable, IntWritable]#Context): Unit = {
      context.write(one, one)
    }
  }

  class IdentityMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
    val one = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
      context.write(new IntWritable(key.toString.toInt), new IntWritable(value.toString.toInt))
    }
  }

  class SumReducer extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {
    val one = new IntWritable(1)

    override def reduce(key: IntWritable, values: lang.Iterable[IntWritable], context: Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {
      context.write(one, new IntWritable(values.asScala.foldLeft(0)(_ + _.get)))
    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job1 = Job.getInstance(configuration,"number unique words job 1")
    job1.setJarByClass(this.getClass)
    job1.setMapperClass(classOf[TokenizerMapper])
    job1.setReducerClass(classOf[OneReducer])
    job1.setInputFormatClass(classOf[TextInputFormat])
    job1.setOutputFormatClass(classOf[TextOutputFormat[IntWritable, IntWritable]])
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[IntWritable])
    job1.setOutputKeyClass(classOf[IntWritable])
    job1.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, new Path(args(1)))
    job1.waitForCompletion(true)

    val job2 = Job.getInstance(configuration, "number unique words job 2")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[IdentityMapper])
    job2.setReducerClass(classOf[SumReducer])
    job2.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job2.setOutputFormatClass(classOf[TextOutputFormat[IntWritable, IntWritable]])
    job2.setOutputKeyClass(classOf[IntWritable])
    job2.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job2, new Path(args(1)))
    FileOutputFormat.setOutputPath(job2, new Path(args(2)))
    System.exit(if (job2.waitForCompletion(true)) 0 else 1)
  }
}
