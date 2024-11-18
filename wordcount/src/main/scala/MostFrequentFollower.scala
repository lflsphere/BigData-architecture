import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala


object MostFrequentFollower {

  class TokenizerMapper extends Mapper[Object, Text, Text, Text] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val words = value.toString.map(_.toLower).replaceAll("[^A-Za-z0-9 ]", "").split(" ")
      for (i <- Range(0, words.length - 1)){
        context.write(new Text(words(i)), new Text(words(i+1)))
      }
    }
  }

  class MostFrequentReducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val maxi = values.asScala.map(x => x.toString).toList.groupBy(x=>x).maxBy(_._2.size)
      context.write(key, new Text(maxi._1))
    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"most frequent follower")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[MostFrequentReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }
}
