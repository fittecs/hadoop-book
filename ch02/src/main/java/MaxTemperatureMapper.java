// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapperのGenerics型はMap関数の<入力キー型, 入力値型, 出力キー型, 出力値型>
 * JavaやScalaのプリミティブに比べて大層な名前になっているが、LongWritableはHadoop版Long、
 * TextはHadoop版Stringのように適当に受け止めてよい。
 * これらの型はネットワーク上でシリアライズする際の効率が良いとのこと。
 */
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  /*
   * 気温が999.9度で登録されている場合、無効なデータとして扱う。
   */
  private static final int MISSING = 9999;

  /**
   * {@link #map(LongWritable, Text, Context)}
   *
   * @param key データファイル内の行数
   * @param value sample.txtの1行分の文字列
   * @param context MapReduce操作Context
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // Mapの入力値にはsample.txtの1行分の文字列が渡される
    String line = value.toString();
    // sample.txtの行の16-19文字目に書かれている観測年を抽出
    String year = line.substring(15, 19);

    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      // 通常気温を数値として抽出
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      // 氷点下の気温を数値として抽出
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    // 気温が無効なデータではない＆品質コードが(0|1|4|5|9)に一致する場合、mapの出力を行う
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      // (観測年, 気温)をmapの結果として出力
      context.write(new Text(year), new IntWritable(airTemperature));
    }
  }
}
// ^^ MaxTemperatureMapper
