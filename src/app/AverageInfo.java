package app;

import akka.actor.ExtendedActorSystem;
import akka.serialization.Serialization;
import app.tools.GetValue;
import app.tools.HBaseClientImpl;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.ToDoubleFunction;


/**
 * Created by lw_co on 2016/9/21.
 * 历史平均温度与风力
 */
public class AverageInfo extends Serialization {

    private static HBaseClientImpl hclient;
    public AverageInfo(ExtendedActorSystem system) {
        super(system);
    }

    //public static Logger logger = LoggerFactory.getLogger(AverageInfo.class);

    public static JavaPairRDD<String, Double[]> dealInfo() throws IOException {
        JavaPairRDD<ImmutableBytesWritable, Result> myRdd= GetValue.getColumnValueFromHB("AverageInfo","CityWeather","cf",24);
        JavaPairRDD<String,Double[]> myRdd1=myRdd.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Double[]>() {
            @Override
            public Tuple2<String, Double[]> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                //String row_id=Bytes.toString(immutableBytesWritableResultTuple2._2().getRow());
                byte[] v;
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("cf"), Bytes.toBytes("Info"));
                JSONObject weatherJson=new JSONObject((new JSONObject(Bytes.toString(v)) ).get("weatherinfo").toString());
                //String[] sval={weatherJson.get("temp").toString(),weatherJson.get("wse").toString(),weatherJson.get("pm").toString()};
                String[] sval={weatherJson.get("temp").toString(),weatherJson.get("wse").toString()};
                String cityName=weatherJson.get("city").toString();
                Double[] data=new Double[sval.length+1];//最后一位在reduce时存出现次数，好求平均
                data[sval.length]=1.0;//最后一位赋1，为reduce用。
                for(int i=0;i<sval.length;++i)
                {
                    if(sval[i]!=null && !"null".equals(sval[i]) && !"?".equals(sval[i]) && !"".equals(sval[i])){
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


        /**topk
        JavaRDD<Integer> rddlist1=myRdd1.map(new Function<Tuple2<String,Double[]>, Integer>() {

            @Override
            public Integer call(Tuple2<String, Double[]> stringTuple2) throws Exception {
                //List list= Arrays.asList(stringTuple2._2);
                return stringTuple2._2[2].intValue();
            }
        });
        class myCom implements Comparator<Integer>,Serializable{

            @Override
            public int compare(Integer o1, Integer o2) {
                return 0;
            }
        }
        rddlist1.takeOrdered(10, new myCom());*/



        System.out.println("ok");

        /** 写hdfs,写下来还是part-00000以文件夹的形式，不同部分
        JavaSparkContext jsc=new JavaSparkContext();
        JavaPairRDD<String,double[]> rddlist1= jsc.parallelizePairs(mylist1);
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(URI.create("hdfs://master:9000/user/root/weatherInfo/tempAndWind1/test.txt"),conf);
        FSDataOutputStream hdfsStream=fs.create(new Path("hdfs://master:9000/user/root/weatherInfo/tempAndWind1/test.txt"));
        hdfsStream.writeChars("lwtestsssss");
        hdfsStream.close();
        fs.close();*/




        //System.out.println(String.join(",", (CharSequence[]) mylist1.toArray()));

        /*JavaRDD<String> re=myRdd1.map(new Function<Tuple2<String, double[]>, String>() {
            @Override
            public String call(Tuple2<String, double[]> stringTuple2) throws Exception {
                String val=stringTuple2._1;
                val=val+","+stringTuple2._2[0]+","+stringTuple2._2[1];
                return val;
            }
        });
        re.coalesce(1).saveAsTextFile("hdfs://master:9000/user/root/weatherInfo/tempAndWind");*/
        return myRdd1;
    }
    public static List<Tuple2<String,Double[]>> rank(JavaPairRDD<String,Double[]> rdd,int n){
        class myComparator implements Comparator<Tuple2<String, Double[]>>,Serializable {


            @Override
            public int compare(Tuple2<String, Double[]> o1, Tuple2<String, Double[]> o2) {
                //Double val=o1._2[n]-o2._2[n];

                //return Double.compare(o1._2[n],o2._2[n];//升序
                return Double.compare(o2._2[n],o1._2[n]);//降序
            }
        }
        List<Tuple2<String,Double[]>> mylist=rdd.takeOrdered(100, new myComparator());
        return  mylist;
    }

    public static void main(String[] args) throws IOException, ParseException {

        JavaPairRDD<String,Double[]> myrdd=dealInfo();

        System.out.println("rank ready");

        String tableName="WeatherInfo";//表名
        String cf="a";//列族
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        String rowKey=sdf.format((new Date())).toString();//rowKey这里为时间

        System.out.println("rank tmp go");

        /**温度*/
        List<Tuple2<String,Double[]>> tempList=rank(myrdd,0);//温度

        System.out.println("rank ws go");
        /**风力*/
        List<Tuple2<String,Double[]>> wsList=rank(myrdd,1);//风力

        System.out.println("rank pm go");
        /**PM*/
        List<Tuple2<String,Double[]>> pmList=rank(myrdd,2);
//        insertHbaseInfo(pmList,2,tableName,rowKey,cf,"pm");

//        /**湿度*/
//        List<Tuple2<String,Double[]>> sdList=rank(myrdd,3);
//        insertHbaseInfo(sdList,3,tableName,rowKey,cf,"sd");
        System.out.println("rank finished");


        hclient=new HBaseClientImpl(tableName);
        /**温度*/
        System.out.println("insert 温度");
        insertHbaseInfo(tempList,0,tableName,rowKey,cf,"tp");
        /**风力*/
        System.out.println("insert 风力");
        insertHbaseInfo(wsList,1,tableName,rowKey,cf,"ws");
        /**PM*/
        System.out.println("insert 温度");
        insertHbaseInfo(pmList,2,tableName,rowKey,cf,"pm");
        System.out.println("AverageInfo be finished");
        hclient.close();


    }
    public static void insertHbaseInfo(List<Tuple2<String,Double[]>> list,int type,String tableName,String rowKey,String cf,String qf){
        List<Put> thePuts=new ArrayList<>();
        for(int i=1;i<list.size()&&i<101;++i){
            Tuple2<String,Double[]> myTup=list.get(i);
            String val=myTup._1+"-"+myTup._2[type];
            //hclient.addRecordwithTs(tableName,rowKey,cf,qf,i,val);

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(qf),i,Bytes.toBytes(val));
            thePuts.add(put);
            System.out.println("insert recored " + rowKey+" : "+qf + " to table " + tableName +" ok.the value="+val);
        }
        hclient.addRecordWithTsPuts(tableName,thePuts);
        return;
    }

    /*public static void insertHbaseInfo(List<Tuple2<String,Double[]>> list,int type,String tableName,String rowKey,String cf,String qf){
        for(int i=1;i<list.size()&&i<101;++i){
            Tuple2<String,Double[]> myTup=list.get(i);
            String val=myTup._1+"-"+myTup._2[type];
            hclient.addRecordwithTs(tableName,rowKey,cf,qf,i,val);
        }
    }*/

}
