import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class WordCount {

    public static class FirstMapper implements FlatMapFunction<String, String> {

        @Override
        public Iterator<String> call(String s) throws Exception {
            return Arrays.asList(s.split("\\s")).iterator();
        }
    }

    public static class SecondMapper implements PairFunction<String, String, Integer> {

        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<String, Integer>(s, 1);
        }
    }

    public static class ReducerFunction implements Function2<Integer, Integer, Integer> {

        @Override
        public Integer call(Integer v1, Integer v2) {
            return v1 + v2;
        }
    }

    public static class SwapMapper implements PairFunction<Tuple2<String, Integer>, Integer, String> {

        @Override
        public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            return stringIntegerTuple2.swap();
        }
    }

    public static class IntegerCompartor implements Comparator<Integer>, Serializable {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o2 - o1;
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext context = new JavaSparkContext(conf);
//        JavaRDD<String> inputs = context.textFile(args[0]);
//        inputs = inputs.flatMap(new FirstMapper());
//        JavaPairRDD<String, Integer> tokenpairs = inputs.mapToPair(new SecondMapper());
//        JavaPairRDD<String, Integer> wordcounts =  tokenpairs.reduceByKey(new ReducerFunction());
//        JavaPairRDD<Integer, String> res = wordcounts.mapToPair(new SwapMapper());
//        res = res.sortByKey(new IntegerCompartor());
//        res.saveAsTextFile(args[1]);

//        context.textFile(args[0])
//                .flatMap(new FlatMapFunction<String, String>() {
//                    @Override
//                    public Iterator<String> call(String s) throws Exception {
//                        return Arrays.asList(s.split("\\s")).iterator();
//                    }
//                })
//                .mapToPair(new PairFunction<String, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(String s) throws Exception {
//                        return new Tuple2<String, Integer>(s, 1);
//                    }
//                })
//                .reduceByKey(new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer v1, Integer v2) {
//                        return v1 + v2;
//                    }
//                })
//                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//                    @Override
//                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                        return stringIntegerTuple2.swap();
//                    }
//                })
//                .sortByKey(new IntegerCompartor())
//                .saveAsTextFile(args[1]);
//        // cannot use anonymous class that implements more than one interfaces

        context.textFile(args[0])
            .flatMap(s -> Arrays.asList(s.split("\\s")).iterator())
            .mapToPair(t -> new Tuple2<>(t, 1))
            .reduceByKey((t1, t2) -> t1 + t2)
            .mapToPair(t -> t.swap())
            .sortByKey(new IntegerCompartor())
            .saveAsTextFile(args[1]);
        // cannot use anonymous class that implements more than one interfaces
    }
}
