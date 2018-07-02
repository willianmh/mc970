import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class WordCount {
  class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  def map(Object key, Text value, Context context) throws
  IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      context.write(word, one);
    }
  }
}
class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
  private IntWritable result = new IntWritable();
  def reduce(Text key, Iterable<IntWritable> values,
    Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  def main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

object SimpleApp {
def main(args: Array[String]) {
// Create a Scala Spark Context. val conf = new SparkConf().setAppName("wordCount") val sc = new SparkContext(conf)
val lines = sc.textFile(inputFile) // cada item do RDD é uma linha do arquivo (String)
val words = lines.flatMap(line => line.split (" ")) // cada item do RDD é uma palavra do arquivo
val intermData = words.map(word => (word,1)) // cada item do arquivo é um par (palavra,1)
val wordCount = intermData.reduceByKey(_ + _) // cada item do RDD contém ocorrência final de cada palavra
val contagens = wordCount.take(5) // 5 resultados no programa driver
} }

object Analisador {

  // Args = path/to/text0.txt path/to/text1.txt
  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Contagem de Palavra"))

    println("TEXT1")

    // read first text file and split into lines
    val lines1 = sc.textFile(args(0))

    println(lines1);

    // TODO: contar palavras do texto 1 e imprimir as 5 palavras com as maiores ocorrencias (ordem DECRESCENTE)
    // imprimir na cada linha: "palavra=numero"


    println("TEXT2")

    // read second text file and split each document into words
    val lines2 = sc.textFile(args(1))

    // TODO: contar palavras do texto 2 e imprimir as 5 palavras com as maiores ocorrencias (ordem DECRESCENTE)
    // imprimir na cada linha: "palavra=numero"

    println("COMMON")

    // TODO: comparar resultado e imprimir na ordem ALFABETICA todas as palavras que aparecem MAIS que 100 vezes nos 2 textos
    // imprimir na cada linha: "palavra"

  }

}
