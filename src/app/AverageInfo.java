package app;

import akka.actor.ExtendedActorSystem;
import akka.serialization.Serialization;
import app.tools.GetValue;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.ToDoubleFunction;


/**
 * Created by lw_co on 2016/9/21.
 * 历史平均温度与风力
 */
public class AverageInfo extends Serialization {
    public AverageInfo(ExtendedActorSystem system) {
        super(system);
    }

    //public static Logger logger = LoggerFactory.getLogger(AverageInfo.class);

    public static void dealInfo() throws IOException {
        JavaPairRDD<ImmutableBytesWritable, Result> myRdd= GetValue.getColumnValueFromHB("AverageInfo","CityWeather","cf",24);
        JavaPairRDD<String,Double[]> myRdd1=myRdd.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Double[]>() {
            @Override
            public Tuple2<String, Double[]> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                //String row_id=Bytes.toString(immutableBytesWritableResultTuple2._2().getRow());
                byte[] v;
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("cf"), Bytes.toBytes("Info"));
                JSONObject weatherJson=new JSONObject((new JSONObject(Bytes.toString(v)) ).get("weatherinfo").toString());
                String[] sval={weatherJson.get("temp").toString(),weatherJson.get("wse").toString()};
                String cityName=weatherJson.get("city").toString();
                Double[] data=new Double[sval.length+1];//最后一位在reduce时存出现次数，好求平均
                data[sval.length]=1.0;//最后一位赋1，为reduce用。
                for(int i=0;i<sval.length;++i)
                {
                    if(sval[i]!=null && !"null".equals(sval[i]) && !"?".equals(sval[i])){
                        data[i]=Double.parseDouble(sval[i]);
                    }
                    else{return null;}
                }

                return new Tuple2<String, Double[]>(cityName,data);
            }
        }).filter(new Function<Tuple2<String, Double[]>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Double[]> stringTuple2) throws Exception {

                return stringTuple2!=null;
            }
        }).reduceByKey(new Function2<Double[], Double[], Double[]>() {
            @Override
            public Double[] call(Double[] ints, Double[] ints2) throws Exception {

                for(int i=0;i<ints.length;++i)
                {
                    ints[i]=ints[i]+ints2[i];
                }
                return ints;
            }
        }).mapValues(new Function<Double[], Double[]>() {
            @Override
            public Double[] call(Double[] ints) throws Exception {

                //logger.info("wwwww:"+Arrays.toString(ints));
                for(int i=0;i<ints.length-1;++i)
                {
                    ints[i]=ints[i]/ints[ints.length-1];
                }
                //System.out.println("wwwww:"+Arrays.toString(ints));
                return ints;
            }
        });

//        JavaPairRDD<String, Integer> rddlist=myRdd1.mapToPair(new PairFunction<Tuple2<String,Double[]>, String, Integer>() {
//
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, Double[]> stringTuple2) throws Exception {
//                List list= Arrays.asList(stringTuple2._2);
//                return new Tuple2<String, Integer>(stringTuple2._1,stringTuple2._2[2].intValue());
//            }
//        });
        JavaRDD<Integer> rddlist1=myRdd1.map(new Function<Tuple2<String,Double[]>, Integer>() {

            @Override
            public Integer call(Tuple2<String, Double[]> stringTuple2) throws Exception {
                //List list= Arrays.asList(stringTuple2._2);
                return stringTuple2._2[2].intValue();
            }
        });
//        JavaPairRDD<String, List<Double>> rddlist=myRdd1.mapToPair(new PairFunction<Tuple2<String,Double[]>, String, List<Double>>() {
//
//            @Override
//            public Tuple2<String, List<Double>> call(Tuple2<String, Double[]> stringTuple2) throws Exception {
//                List list= Arrays.asList(stringTuple2._2);
//                return new Tuple2<String, List<Double>>(stringTuple2._1,list);
//            }
//        });
//        List<Tuple2<String,Integer>> mylist1=rddlist.takeOrdered(10, new Comparator<Tuple2<String, Integer>>() {
//            @Override
//            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
//                return o1._2-o2._2;
//            }
//
//
//        });


        class myCom implements Comparator<Integer>,Serializable{

            @Override
            public int compare(Integer o1, Integer o2) {
                return 0;
            }
        }
        rddlist1.takeOrdered(10, new myCom());
//        List<Tuple2<String,Integer>> mylist1=rddlist.takeOrdered(10,new Comparator<Tuple2<String, Integer>>() {
//
//            @Override
//            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
//                return 0;
//            }
//        });
        System.out.println("ok");
//        List<Tuple2<String,Double[]>> mylist1=myRdd1.takeOrdered(100, new Comparator<Tuple2<String, Double[]>>() {
//            //排序温度
//            @Override
//            public int compare(Tuple2<String, Double[]> o1, Tuple2<String, Double[]> o2) {
////                if(o1._2[1]>o2._2[1])
////                {
////                    return 1;
////                }else if(o1._2[1]<o2._2[1])
////                {
////                    return -1;
////                }
//                //Double val=o1._2[1]-o2._2[1];
//                return 0;
//            }
//        });
//        JavaSparkContext jsc=new JavaSparkContext();
//        JavaPairRDD<String,double[]> rddlist1= jsc.parallelizePairs(mylist1);
//        Configuration conf=new Configuration();
//        FileSystem fs=FileSystem.get(URI.create("hdfs://master:9000/user/root/weatherInfo/tempAndWind1/test.txt"),conf);
//        FSDataOutputStream hdfsStream=fs.create(new Path("hdfs://master:9000/user/root/weatherInfo/tempAndWind1/test.txt"));
//        hdfsStream.writeChars("lwtestsssss");
//        hdfsStream.close();
//        fs.close();

        /*List<Tuple2<String,double[]>> mylist2=myRdd1.takeOrdered(100, new Comparator<Tuple2<String, double[]>>() {
            //排序风度
            @Override
            public int compare(Tuple2<String, double[]> o1, Tuple2<String, double[]> o2) {
                if(o1._2[2]>o2._2[2])
                {
                    return 1;
                }else if(o1._2[2]<o2._2[2])
                {
                    return -1;
                }
                return 0;
            }
        });*/

        /*JavaRDD<String> re=myRdd1.map(new Function<Tuple2<String, double[]>, String>() {
            @Override
            public String call(Tuple2<String, double[]> stringTuple2) throws Exception {
                String val=stringTuple2._1;
                val=val+","+stringTuple2._2[0]+","+stringTuple2._2[1];
                return val;
            }
        });
        re.coalesce(1).saveAsTextFile("hdfs://master:9000/user/root/weatherInfo/tempAndWind");*/
    }
    public static void main(String[] args) throws IOException {
        dealInfo();
    }

}
