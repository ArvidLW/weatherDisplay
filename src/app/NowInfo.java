package app;

import app.tools.GetValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by lw_co on 2016/9/21.
 * 当天那时的PM，温度，风力，风向，湿度
 * 每个城市取当天最新的一条
 * getColumnValueFromHB1
 */
public class NowInfo {
    public static void dealNowInfo() throws IOException {
        JavaPairRDD<ImmutableBytesWritable,Result> rdd= GetValue.getColumnValueFromHB1("NowInfo","CityWeather","cf",1);
        JavaRDD<String> rdd1=rdd.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
            @Override
            public String call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                byte[] v;
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("cf"), Bytes.toBytes("Info"));
                JSONObject weatherJson=new JSONObject((new JSONObject(Bytes.toString(v)) ).get("weatherinfo").toString());
                String sval=weatherJson.get("pm").toString()+","
                        +weatherJson.get("temp").toString()+","
                        +weatherJson.get("wse").toString()+","
                        +weatherJson.get("wd").toString();
                String cityName=weatherJson.get("city").toString();

                sval=cityName+","+sval;

                return sval;
            }
        });
        rdd1.coalesce(1).saveAsTextFile("hdfs://master:9000/user/root/weatherInfo/nowWeatherInfo");
    }
    public static void main(String[] args) throws IOException {
        dealNowInfo();
    }
}
