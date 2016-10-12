//package app;

/**
 * Created by JZ50220 on 2016/9/21.
 */


import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.col;
import scala.Tuple2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class RecommenderSystems
{
    static final String USER = "wyc";
    private static final Pattern COMMA = Pattern.compile("comma");

    public static class user_log implements Serializable {
        private int user_id;
        private int item_id;
        //private int cat_id;
        private int rating;

        @Override
        public String toString() {
            return "user_log [user_id=" + user_id + ", item_id=" + item_id + ", rating=" + rating + "]";
        }
        public int getUserID() {
            return user_id;
        }

        public void setUserID(int userID) {
            this.user_id = userID;
        }

        public int getItemID() {
            return item_id;
        }

        public void setItemID(int itemID) {
            this.item_id = itemID;
        }

        public int getRating() {
            return rating;
        }

        public void setRating(int rating) {
            this.rating = rating;
        }
    }

    public static class ModelResult{
        public int rank;
        public int numIter;
        public double lambda;
        public double mse;

        public ModelResult(int iRank, int iNumIter, double dLambda, double dMse){
            rank = iRank;
            numIter = iNumIter;
            lambda = dLambda;
            mse = dMse;
        }
    }


    public static MatrixFactorizationModel buildModel(RDD<Rating> rdd, int rank, int numIter, double lambda)
    {
        /*训练模型 矩阵分解模型*/
        /*暂时采用一组数据，可用多组数据*/

        /*int rank = 10;  *//*特征数*//*
        int numIter = 20; *//*迭代次数*//*
        double lambda = 0.01;*/
        MatrixFactorizationModel model = ALS.train(rdd, rank, numIter, lambda);
        return model;
    }

    public static RDD<Rating>[] splitData(JavaRDD<String> lines) {
        /*分割数据，一部分用于训练，一部分用于测试
        * 分割标准待定
        * */
        /*SparkConf sparkConf = new SparkConf().setAppName("JavaALS").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("/home/nodin/ml-100k/u.data");*/

        JavaRDD<Rating> ratings = lines.map(line -> {
            String[] tok = COMMA.split(line);
            int x = Integer.parseInt(tok[0]);
            int y = Integer.parseInt(tok[1]);
            double rating = Double.parseDouble(tok[2]);

            return new Rating(x, y, rating);
        });

        RDD<Rating>[] splits = ratings.rdd().randomSplit(new double[]{0.6,0.4}, 11L);
        return splits;
    }

    public static void recommender() throws IOException {
        String table = "recommender systems";

        /*首先，设置spark配置。 设置应用名称，就是在spark web端显示的应用名称，当然还可以设置其它的，在提交的时候可以指定。*/
        SparkConf confsp = new SparkConf()
                .setAppName("SparkHBaseTest_CommRcmdDemo")   /*设置应用名称；*/
                .setMaster("local[4]"); /*设置master，缺省将读取环境配置中的设置。 local[4] 表示本地4个线程运行；*/

        /*创建spark操作环境对象*/
        JavaSparkContext sc = new JavaSparkContext(confsp);
        sc.setLogLevel("WARN");

        System.out.println("ready to read.");

        String file_input  = "/home/houlinli/prj/data/user_log_format1.csv";
        //String file_input  = "/comm_rcmd/data/user_log_format1.csv";
        String file_output = "/home/houlinli/prj/data/user_log_format_result.csv";

        /*1.数据准备*/
        /*1.1 读入数据*/
        DataFrame dataFrameAll = getDataFrame(sc, file_input);

        /*1.2 数据预处理*/
        /*取出user_id,item_id,rating等多列数据，并去重(3)*/
        /*数据特点：同一个时间戳里，user_id对item_id，可能有多个操作，因此产生多条记录*/
//        DataFrame dataFramePre = dataFrameAll.select( col("user_id"), col("item_id"), col("cat_id"), col("brand_id"),
//                col("time_stamp"), col("action_type"), col("rating") ).distinct();
//        dataFramePre.registerTempTable("dftmp");  /*在内存中创建临时表*/
//        dataFramePre.printSchema();
//        System.out.printf("dataFramePre.count = %d\n", dataFramePre.count());
//        saveDataFrame(dataFramePre, "dataFramePre.csv", "false");

//
//        SQLContext sqlContext = dataFramePre.sqlContext();
//        sqlContext.sql("select * from dftmp").registerTempTable("temp1");
//        DataFrame dataFrameTmp1 = sqlContext.sql(
///*                "select a.user_id, a.item_id, a.rating " +
//                "from temp1 as a, (select * from temp1 where rating in " +
//                        "(select max(rating) from temp1 where user_id=a.user_id and item_id=a.item_id))"*/
//                "SELECT DISTINCT * from temp1 as a where rating in (select max(rating) from temp1 where user_id=a.user_id and item_id=a.item_id)"
//                /*"select distinct * from temp1 as a " +
//                        " where rating in " +
//                        " (select max(rating) from temp1 where user_id=a.user_id and item_id=a.item_id)"*/
//        );
//
//       /* "SELECT a.pin, logon, offline, `online`, suspend, busy, rest, idle, b.cnName  \n" +
//                "FROM status AS a,    \n" +
//                "            (select *   \n" +
//                "            from servers   \n" +
//                "            where id in (select max(id)   \n" +
//                "                        from server   \n" +
//                "                        group by pin)  \n" +
//                "            ) b  \n" +
//                "WHERE a.pin = b.pin   \n" +
//                "      AND a.created >= #beginTime#   \n" +
//                "      AND a.created < #endTime#  "*/
//        //sqlContext.sql().registerTempTable("temp3");
//        System.out.printf("dataFrameTmp1.count = %d\n", dataFrameTmp1.count());
////        saveDataFrame(dataFrameTmp1, "dataFrameTmp2.csv", "false");
//
//
//        String duplicates[] = {"user_id","item_id"};
//        DataFrame dataFrameSql = dataFramePre.dropDuplicates(duplicates);
//                /*sqlContext.sql(
//    "delete a from dftmp a where rating not in (select max(rating) from dftmp where user_id=a.user_id and item_id=a.item_id)"
//                                );    数据过滤*/
//
//        dataFrameSql.printSchema();
//        System.out.printf("dataFrameSql.count = %d\n", dataFrameSql.count());
//
////        saveDataFrame(dataFrameSql, "dataFrameSql.csv", "false");
//
//        //dataFramePre.toJavaRDD().distinct();
//
//        System.out.println(" process over.");



        /*对取出的数据进行处理，包括：去重、清理等*/
/*
        DataFrame dataFrameDistinct = dataFramePre.distinct();

        dataFrameDistinct.printSchema();
        System.out.printf("distinct dataFramePre.count = %d\n", dataFrameDistinct.count());
*/

        /**全部数据*/
        //char charTmp = (char)System.in.read();


        /*2.分割数据*/
        /*分割数据比例：{训练：测试：推荐：保留} = {0.1:0.1:0.3:0.5}*/
        double split_factor[] = {0.1, 0.1, 0.3, 0.5};
        //分割为4个DataFrame,DataFrame相当于Rdd
        DataFrame[] dataFrames = dataFrameAll.randomSplit(split_factor, 11L);

        /*训练数据：生成最佳模型参数*/
        JavaRDD<Rating> training_JavaRDD = getRatingJavaRDD(dataFrames[0]);
        /*验证数据*/
        JavaRDD<Rating> validation_for_predict_JavaRDD = getRatingJavaRDD(dataFrames[1]);

        /**生成训练数据：生成最佳推荐模型*/
        JavaRDD<Rating> predict_JavaRDD = getRatingJavaRDD(dataFrames[2]);

        /*3.模型训练，获取最佳模型参数*/
        //ModelResult modelResult_best = getBestModelResult(training_JavaRDD, validation_for_predict_JavaRDD);
        //结点相关参数
        ModelResult modelResult_best = new ModelResult(20, 10, 0.01, 0.595351);

        /*4.预测*/
        /*4.1 准备数据*/
        double predict_split[] = {0.7, 0.3};

        JavaRDD<Rating> test[] = predict_JavaRDD.randomSplit(predict_split, 11L);

        /*4.2 使用最优模型参数进行推荐模型训练*/
        MatrixFactorizationModel model_best = buildModel(JavaRDD.toRDD(predict_JavaRDD),
                modelResult_best.rank,
                modelResult_best.numIter,
                modelResult_best.lambda);
        /*4.3 计算RMSE*/
        double mse_best = getMse(predict_JavaRDD, model_best);

        /*4.4 使用最优模型对指定user进行预测*/
        JavaRDD<Tuple2<Object, Object>> userProducts = predict_JavaRDD.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                });

        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model_best.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
                                return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
                            }
                        }
                ));

        System.out.printf("predict_JavaRDD.count = %d, predictions.count = %d\n",
                predict_JavaRDD.count(), predictions.count());
        /**存储预测结果*/
        predict_JavaRDD.saveAsTextFile("prediction_data.csv");
        predictions.saveAsTextFile("predictions_result.csv");


        /*4.5计算均方差mse与均方根误差rmse*/
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
                JavaPairRDD.fromJavaRDD(predict_JavaRDD.map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
                                return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
                            }
                        }
                )).join(predictions).values();
        //join是两个key作交集
        double predict_m = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();//计算RDD元素的均值，这里是计算mse

        System.out.printf("prediction.mse = %f\n", Math.sqrt(predict_m));
        /*5.推荐*/
        /*5.1使用预测结果，采用某些规则进行推荐，Top20？*/

        System.out.println("over.");

        sc.stop();
    }
    /**获得rating,指定相应的列，rating也相当于key-row的表*/
    private static JavaRDD<Rating> getRatingJavaRDD(DataFrame dataFrame) {
        return dataFrame.toJavaRDD().map(new Function<Row, Rating>() {
            @Override
            public Rating call(Row row) throws Exception {
                return new Rating(row.getInt(0), row.getInt(1), row.getInt(7));
            }
        });
    }
    public static ModelResult getBestModelResult(JavaRDD<Rating> training_JavaRDD,
                                                 JavaRDD<Rating> validation_for_predict_JavaRDD) throws IOException {
        int ranks[] = {10, 20, 25, 30, 35, 40, 50, 60, 70, 80, 90, 100};
        int numIters[] = {10, 15, 20, 25};
        /*int ranks[] = {10, 20};
        int numIters[] = {10, 20};*/
        double lambdas[] = {0.001, 0.005, 0.007, 0.01, 0.02, 0.05, 0.07, 0.1};
        double RMSE = 0;
        int iCount = 0;

        int rank_best = 0, numIter_best = 0;
        double lambda_best = 0, RMSE_min = 1;

        List<ModelResult> modelResultList = new ArrayList<ModelResult>();

        for (double lambda : lambdas){
            for (int rank : ranks){
                for (int numIter : numIters){

                    /*开始训练*/
                    RMSE = modelTraining(training_JavaRDD, validation_for_predict_JavaRDD, rank, numIter, lambda);

                    ModelResult modelResult = new ModelResult(rank, numIter, lambda, RMSE);
                    modelResultList.add(modelResult);

                    if (RMSE < RMSE_min){

                        RMSE_min = RMSE;
                        rank_best = rank;
                        numIter_best = numIter;
                        lambda_best = lambda;

                        System.out.printf("local best: rank = %d, numIter = %d, lambda = %f, RMSE = %f\n",
                                rank, numIter, lambda, RMSE );
                    }
                    iCount++;
                    System.out.printf("progress: %d/%d\n",
                            lambdas.length * ranks.length * numIters.length, iCount);
                }
            }
        }

        /*保存训练结果到TXT文件*/
        PrintWriter outFile = new PrintWriter(new File("training_Result.txt").getAbsoluteFile());
        outFile.format("training count = %d, validation test count = %d\n",
                training_JavaRDD.count(),
                validation_for_predict_JavaRDD.count() );

        outFile.format("rank_best = %d, numIter_best = %d, lambda_best = %f, RMSE_min = %f\n",
                rank_best, numIter_best, lambda_best, RMSE_min );

        outFile.print("model result cout: ");
        outFile.format("%d\n", modelResultList.size());
        outFile.format("%-4s\t%-7s\t%-7s\t\t%-7s\n", "rank", "numIter", "lambda", "mse");

        for (int i = 0; i < modelResultList.size(); i++){
            ModelResult modelResult = modelResultList.get(i);
            outFile.format("%-4d\t%-7d\t%-7f\t%-7f\n",
                    modelResult.rank, modelResult.numIter, modelResult.lambda, modelResult.mse);
        }
        outFile.close();

        /*命令行显示训练结果和评价结果(RMSE)*/
        System.out.printf("training count = %d, validation test count = %d\n",
                training_JavaRDD.count(),
                validation_for_predict_JavaRDD.count());
        System.out.printf("rank_best = %d, numIter_best = %d, lambda_best = %f, RMSE_min = %f\n",
                rank_best, numIter_best, lambda_best, RMSE_min );

        return  new ModelResult(rank_best, numIter_best, lambda_best, RMSE_min);
    }

    private static double modelTraining(JavaRDD<Rating> training_JavaRDD,
                                        JavaRDD<Rating> validation_for_predict_JavaRDD,
                                        int rank, int numIter, double lambda) {
        /*模型训练*/
        MatrixFactorizationModel model = buildModel(JavaRDD.toRDD(training_JavaRDD), rank, numIter, lambda);

        /*计算MSE(Mean Squared Error，均方差)和RMSE*/
        double MSE = getMse(validation_for_predict_JavaRDD, model);
        double RMSE = Math.sqrt(MSE);

        return RMSE;
    }

    private static JavaRDD<user_log> getUserLog(DataFrame dataFrame) {
        JavaRDD<user_log> persons = dataFrame.toJavaRDD().map(new Function<Row,user_log>(){
            @Override
            public user_log call(Row row) throws Exception
            {
                user_log userLog = new user_log();

                userLog.setUserID(row.getInt(0));
                userLog.setItemID(row.getInt(1));
                userLog.setRating(row.getInt(7));

                return userLog;
            }
        });

        System.out.println("JavaRDD<user_log> persons count:" + persons.count());

        return persons;
    }

    private static double getMse(JavaRDD<Rating> rating, MatrixFactorizationModel model) {
        // Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts = rating.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                }
        );
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
                                return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
                            }
                        }
                ));
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
                JavaPairRDD.fromJavaRDD(rating.map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
                                return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
                            }
                        }
                )).join(predictions).values();

        return JavaDoubleRDD.fromRDD(ratesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();
    }

    private static void saveDataFrame(DataFrame dataFrame, String file_output, String sHeader){
        dataFrame.write()
                .format("com.databricks.spark.csv")
                .option("header", sHeader)   /*true:保存字段名*/
                .save(file_output);

        System.out.println("save count:" + dataFrame.count());
        //dataFrame.show();
    }

    private static DataFrame getDataFrame(JavaSparkContext sc, String file_input) {
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")   //
                .load(file_input);
        df.printSchema();               /*显示字段概要*/

        System.out.println("get count:" + df.count());

        /*df.registerTempTable("dftmp");  *//*在内存中创建临时表*//*
        return sqlContext.sql("SELECT * FROM dftmp WHERE user_id = 328862");    *//*数据过滤*/
        return df;
    }

    public static void main(String[] args) throws IOException
    {
        recommender();
        System.out.println("hello world.\n");
    }
}
