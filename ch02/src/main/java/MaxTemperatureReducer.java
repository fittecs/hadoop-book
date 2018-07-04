// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Mapperの関数と同様ReducerもGenericsで<入力キー型, 入力値型, 出力キー型, 出力値型>を指定する。
 * Reducerの入力キー型, 入力値型はMapperの出力キー型, 出力値型と一致していなければいけない。
 *
 */
public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * {@link #reduce(Text, Iterable, Context)}
   *
   * @param key 観測年
   * @param values 観測年に紐づく気温のリスト(シャッフルで生成される)
   * @param context MapReduce操作Context
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {

    // 最大値を同じ年の値(気温)リストから探索
    int maxValue = Integer.MIN_VALUE;
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
    }
    // ある年の最高気温を出力
    context.write(key, new IntWritable(maxValue));
  }
}
// ^^ MaxTemperatureReducer
