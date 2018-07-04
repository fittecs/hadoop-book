 // cc MaxTemperature Application to find the maximum temperature in the weather dataset
 // vv MaxTemperature
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 /**
 * JavaやMaven、Hadoopがインストール＆環境変数が設定された状態で、hadoop-book直下に移動してパッケージングを行ったのち、
 * 以下のコマンドを実行するとこのクラスが実行される。
 * https://qiita.com/fittecs/items/ed8c75534e5b93e5a4d2
 * の手順を行ってパッケージングまでは完了している前提で話す。
 *
 * $ mvn package -DskipTests -Dhadoop.distro=apache-2
 * $ cd ~/Documents/workspace/hadoop-book
 * $ export HADOOP_CLASSPATH=hadoop-examples.jar
 * $ hadoop MaxTemperature input/ncdc/sample.txt output
 */
public class MaxTemperature {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }

    // oHadoopジョブの実行情報を扱うオブジェクト
    Job job = new Job();
    job.setNumReduceTasks(2);

    //
    job.setJarByClass(MaxTemperature.class);
    // Jobの名前、UIなどに表示されたりする名前
    job.setJobName("Max temperature");

    // デフォルト設定のままHadoopを使うと、引数のinput/ncdc/sample.txtやoutputはローカルの相対パスとして認識される。
    // 出力先のスキーマをhdfs, s3, gsにしてHadoopに適切な設定を行えば出力先を変えることも出来る。

    // ディレクトリやファイルのパターンを指定することも出来る。
    // addInputPath(Job, Path)は複数回呼び出して複数ソースからデータを読み込める。
    FileInputFormat.addInputPath(job, new Path(args[0]));

    // setOutputPath(Job, Path)にはRecuceの出力結果を書き込むディレクトリを指定
    // データの消失を避けるためすでにディレクトリが存在する場合、例外が発生。
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Map処理を行うクラス
    job.setMapperClass(MaxTemperatureMapper.class);
    // Reduce処理を行うクラス
    job.setReducerClass(MaxTemperatureReducer.class);

    // mapの出力型とreduceの出力型が異なる場合、以下のメソッドでmapの出力型も指定する。
    //job.setMapOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(IntWritable.class);

    // reduceの出力型を定義
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);


    // MapReduceジョブ実行プロセスの終了。
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperature
