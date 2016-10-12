package app;

import app.tools.GetValue;
import app.tools.HBaseClientImpl;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by lw_co on 2016/9/21.
 */
public class PM25Ratio {
    //static List<Put> theputs;//这个不申明成静态的就会出现在rdd.foreach 中add后，后面又被释放了的那个情况，有点坑哦
    //static int i;
    public static void pmRadioInfo() throws IOException {
        JavaPairRDD<ImmutableBytesWritable,Result> rdd= GetValue.getColumnValueFromHB1("pmRadio","CityWeather","cf",1);
        JavaPairRDD<String,Integer> rdd1=rdd.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                byte[] v;
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("cf"), Bytes.toBytes("Info"));
                JSONObject weatherJson=new JSONObject((new JSONObject(Bytes.toString(v)) ).get("weatherinfo").toString());
                String pmLevel=weatherJson.get("pm-level").toString();
                if(pmLevel!=null && !"null".equals(pmLevel) && !"?".equals(pmLevel) && !"".equals(pmLevel))
                {
                    return new Tuple2<String, Integer>(pmLevel,1);
                }
                return null;
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2!=null;
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        JavaRDD<String> re=rdd1.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._1+":"+stringIntegerTuple2._2.toString();
            }
        });


        String cf="a";//列族
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        String rowKey=sdf.format((new Date())).toString();//rowKey这里为时间
        String qf="pr";//pm radio
        String tableName="WeatherInfo";
        //final int[] i = {1,2,3,4,5,6};
        //theputs= new ArrayList<Put>();
        //i=0;

        List<String> pmlist=re.collect();
        String val=null;
        for (String s:pmlist) {
            if(val!=null)
            {
                val=val+";"+s;
            }else {
                val=s;
            }
        }
        /*re.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                if(val!=null)
                {
                    val=val+";"+s;
                }else {
                    val=s;
                }

                *//*Put put =new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(qf),++i,Bytes.toBytes(s));

                theputs.add(put);*//*
                System.out.println("wwwwwwwwww:"+rowKey+":"+s);
            }
        });*/
        Put put=new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(qf),Bytes.toBytes(val));

        HBaseClientImpl hclient=new HBaseClientImpl(tableName);
        //System.out.println("wwwwwwwwww-size:"+theputs.size());
        hclient.addRecordWithTsPut(tableName,put);
        hclient.close();
        //hclient.addRecordWithTsPut(tableName,theputs.get(0));
        //hclient.addRecordwithTs(tableName,rowKey,cf,qf,111,"sssssssssssss");
        System.out.println("PM25Ratio-ok");
        //rdd1.coalesce(1).saveAsTextFile("hdfs://master:9000/user/root/weatherInfo/nowWeatherInfo");
    }
    public static void main(String[] args) throws IOException {
        pmRadioInfo();
    }
}
