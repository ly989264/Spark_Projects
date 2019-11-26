import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class WordCount_v2 {

    public static class SelfComparator implements Comparator<String>, Serializable {


        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word count version 2").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> input = jsc.textFile("input.txt");
        input.collect().forEach(System.out::println);
//        JavaRDD<String> step1 = input.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split("\\s")).iterator();
//            }
//        });
        JavaRDD<String> step1 = input.flatMap(s -> Arrays.asList(s.split("\\s")).iterator());
        step1.collect().forEach(System.out::println);
//        JavaPairRDD<String, Integer> step2 = step1.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<>(s, 1);
//            }
//        });
        JavaPairRDD<String, Integer> step2 = step1.mapToPair(s -> new Tuple2<>(s, 1));
        step2.collect().forEach(System.out::println);
//        JavaPairRDD<String, Integer> step3 = step2.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
//                int sum = 0;
//                for (int each : stringIterableTuple2._2) {
//                    sum += each;
//                }
//                return new Tuple2<>(stringIterableTuple2._1, sum);
//            }
//        });
        JavaPairRDD<String, Integer> step3 = step2.groupByKey().mapToPair(stringIterableTuple2 -> {
            int sum = 0;
            for (int each : stringIterableTuple2._2) {
                sum += each;
            }
            return new Tuple2<>(stringIterableTuple2._1, sum);
        });
        step3.collect().forEach(System.out::println);
        JavaPairRDD<String, Integer> step4 = step3.sortByKey(new SelfComparator());
        step4.collect().forEach(System.out::println);
//        JavaPairRDD<Integer, String> step5 = step3.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//            @Override
//            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
//            }
//        });
        JavaPairRDD<Integer, String> step5 = step3.mapToPair(stringIntegerTuple2 -> new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1));
        step5.collect().forEach(System.out::println);
    }
}
