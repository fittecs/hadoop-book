// cc ConfigurationPrinter An example Tool implementation for printing the properties in a Configuration
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

/**
 * MapReduceコマンドラインアプリケーションはextends Configured implements Toolを定義したのち、
 * オーバーライドしたrunメソッドに実行する処理を記述するだけ。
 * Configuredクラスの継承するだけで、-confオプションが使えるようになる。
 *
 * [実行]
 * $ hadoop-book
 * $ mvn compile
 * $ export HADOOP_CLASSPATH=ch05/target/classes/
 * $ hadoop ConfigurationPrinter -conf conf/hadoop-local.xml
 * $ hadoop ConfigurationPrinter -conf conf/hadoop-localhost.xml
 * $ hadoop ConfigurationPrinter -conf hadoop-localhost.xml | grep mapred.job.tracker
 *
 * ちなみにhadoopコマンドで-Dオプションを使用すると設定値をコマンド実行時にする。
 *
 * $ hadoop ConfigurationPrinter | grep color
 * $ hadoop ConfigurationPrinter -Dcolor=yellow | grep color
 *
 * 本書では-Dとプロパティの間にスペース空けないとダメと書いているが、空けなくてもいける気がする。
 */
public class ConfigurationPrinter extends Configured implements Tool {

  // コマンドが実行されたタイミングで、HDFSの設定とMapReduceの設定をConfigurationクラスで読み書きできるようにする。
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    // 上記定義で読み込んだ設定値を1件づつ取り出して標準出力に表示している
    for (Entry<String, String> entry: conf) {
      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // ToolRunner.runはこのクラスのrunメソッドを実行しているだけ。
    int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
    System.exit(exitCode);
  }
}
// ^^ ConfigurationPrinter
