import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class Assignment1 {

    public static class Triple implements Serializable {
        // create own Iterable class
        String user;
        Integer rating1;
        Integer rating2;

        public Triple(String user, Integer rating1, Integer rating2) {
            this.user = user;
            this.rating1 = rating1;
            this.rating2 = rating2;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();// use StringBuilder is better
            sb.append('(').append(user).append(',').append(rating1).append(',')
                    .append(rating2).append(')');
            return sb.toString();
        }


    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Assignment1").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("movies-ratings.txt");

//        input.collect().forEach(System.out::println);  // used for testing

        JavaPairRDD<String, Tuple2<String, Integer>> step1 = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(String s) throws Exception {
                String[] arr = s.split("::");
                String user = arr[0];
                String movie = arr[1];
                Integer rating = Integer.parseInt(arr[2]);  // can I use int here?
                return new Tuple2<String, Tuple2<String, Integer>>(user, new Tuple2<>(movie, rating));
            }
        });
        // in Spark, tuple will show as parentheses, and it cannot be changed

        // note that in Hadoop, it will automatically do the group work
        // but in Spark, you have to explicitly use the groupByKey method
        JavaPairRDD<Tuple2<String, String>, Triple> step2 = step1.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String, String>, Triple>() {
            @Override
            public Iterator<Tuple2<Tuple2<String, String>, Triple>> call(
                    Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) throws Exception {
                ArrayList<Tuple2<Tuple2<String, String>, Triple>> ret = new ArrayList<>();
                String user = stringIterableTuple2._1;
                ArrayList<Tuple2<String, Integer>> movies = new ArrayList<>();
//                for(Tuple2<String, Integer> t : stringIterableTuple2._2) {
//                    movies.add(t);
//                } // equivalent to the following
                stringIterableTuple2._2.forEach(movies::add);
                for (int i = 0; i < movies.size() - 1; i++) {
                    for (int j = i + 1; j < movies.size(); j++) {
                        String movie1 = movies.get(i)._1;
                        Integer rating1 = movies.get(i)._2;
                        String movie2 = movies.get(j)._1;
                        Integer rating2 = movies.get(j)._2;
                        Tuple2<String, String> moviepair;
                        Triple triple;
                        if (movie1.compareTo(movie2) > 0) {
                            moviepair = new Tuple2<>(movie1, movie2);
                            triple = new Triple(user, rating1, rating2);
                        } else {
                            moviepair = new Tuple2<>(movie2, movie1);
                            triple = new Triple(user, rating2, rating1);
                        }
                        ret.add(new Tuple2<>(moviepair, triple));
                    }
                }
                return ret.iterator();
            }
        });
        // Note: if the input is a single object and the output is also a single object, use map function (map or maptopair)
        // If the input is a single object, while the output is multiple objects, use flatmap or flatmaptopair function
        // flatmap?

        step2.groupByKey().saveAsTextFile("output");
    }
}
