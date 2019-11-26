import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;

public class Assignment1_v2S {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Assignment 1 version 2").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> input = context.textFile("movies-ratings.txt");

        JavaPairRDD<String, Tuple2<String, Integer>> step1 = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(String s) throws Exception {
                String[] str_arr = s.split("::");
                return new Tuple2<>(str_arr[0], new Tuple2<>(str_arr[1], Integer.parseInt(str_arr[2])));
            }
        });
        step1.collect().forEach(System.out::println);

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> step2 = step1.groupByKey();
        step2.collect().forEach(System.out::println);

        JavaPairRDD<Tuple2<String, String>, Tuple3<String, Integer, Integer>> step3 = step2.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String, String>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Iterator<Tuple2<Tuple2<String, String>, Tuple3<String, Integer, Integer>>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) throws Exception {
                        String user = stringIterableTuple2._1;
                        ArrayList<Tuple2<Tuple2<String, String>, Tuple3<String, Integer, Integer>>> res = new ArrayList<>();
                        ArrayList<Tuple2<String, Integer>> arr = new ArrayList<>();
                        for (Tuple2<String, Integer> each : stringIterableTuple2._2) {
                            arr.add(each);
                        }
                        for (int i = 0; i < arr.size() - 1; i++) {
                            for (int j = i + 1; j < arr.size(); j++) {
                                Tuple2<String, String> moviepair;
                                Tuple3<String, Integer, Integer> userratings;
                                String m1 = arr.get(i)._1;
                                Integer r1 = arr.get(i)._2;
                                String m2 = arr.get(j)._1;
                                Integer r2 = arr.get(j)._2;
                                if (m1.compareTo(m2) > 0) {
                                    moviepair = new Tuple2<>(m1, m2);
                                    userratings = new Tuple3<>(user, r1, r2);
                                } else {
                                    moviepair = new Tuple2<>(m2, m1);
                                    userratings = new Tuple3<>(user, r2, r1);
                                }
                                res.add(new Tuple2<>(moviepair, userratings));
                            }
                        }
                        return res.iterator();
                    }
                }
        );
        step3.collect().forEach(System.out::println);

        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple3<String, Integer, Integer>>> step4 = step3.groupByKey();
        step4.collect().forEach(System.out::println);
    }
}

// note that after group by key, the value part should be the Iterable
// but after flat map to pair, the value part should not be the Iterable
