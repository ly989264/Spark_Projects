/* Documentation (z5190675, Yu Liu)
 * The basic idea of my implementation is that, at the first beginning, all nodes except the starting node will have a
 * value of -1 (means not reached yet), and the starting node with have a value of 0.
 * The program will iterate several times, and each iteration tends to "find" some nodes.
 * At each iteration, each node will generate a set of pairs of the value for all its pointing-to-nodes, for example,
 * if the node A can point to node B with distance of 1 and node C with distance of 2, and the value of node A is 3,
 * then in this iteration, node A will generate the pair of (B, 4) and (C, 5).
 * After that, all these pairs will be grouped by key, then these results will be "reduced" to update the value of each node.
 * And these updated value for each node will then be passed to the next iteration.
 * This is just a simplified idea of my implementation, in fact, the program also need to consider some other issues,
 * such as the route of the node, and the usage of left out join to avoid the vanish of nodes.
 * When all nodes are "stable", i.e., the values will not change for several iteration steps, it is time to quit the loop.
 * But here remains the issue that the nodes that have no out edges will not be included,
 * so in this case, the program will use the right outer join to join the result and an RDD which contains all nodes.
 * After that, the program will sort these nodes based on the distance, remove the starting node,
 * and turn it into a class named Node, which will properly do the output and avoid the parentheses in the final output.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class Assignment2_v {

    // global variables, just used to control the iteration state
    public static boolean isChanged = true;
    public static int count = 0;

    // self-defined class used to store the final result for each node, rewrite the toString method
    public static class Node implements Serializable {
        private String nodeName;
        private String route;
        private Integer distance;

        public Node() {
            this.nodeName = "";
            this.route = "";
            this.distance = -1;  // initialize distance to be -1, means not reached yet
        }

        public Node(String node_name) {
            this();
            this.nodeName = node_name;
        }

        public Node(String node_name, String route, Integer distance) {
            this.nodeName = node_name;
            this.route = route;
            this.distance = distance;
        }

        public String getNodeName() {
            return nodeName;
        }

        public void setNodeName(String nodeName) {
            this.nodeName = nodeName;
        }

        public String getRoute() {
            return route;
        }

        public void setRoute(String route) {
            this.route = route;
        }


        public Integer getDistance() {
            return distance;
        }

        public void setDistance(Integer distance) {
            this.distance = distance;
        }

        @Override
        public String toString() {
            // return the proper format of output
            StringBuilder builder = new StringBuilder();
            builder.append(this.nodeName).append(",").append(this.distance).append(",").append(this.route);
            return builder.toString();
        }
    }

    // comparator used in sort by key to compare integers
    // assume -1 means infinity, so -1 should appear at the end of the file
    public static class IntegerComparator implements Comparator<Integer>, Serializable {
        @Override
        public int compare(Integer o1, Integer o2) {
            // assume -1 appears at the bottom of the file
            if (o1 == -1 && o2 == -1) {
                return 0;
            }
            if (o1 == -1) {
                return 1;
            } else if (o2 == -1) {
                return -1;
            }
            return o1.compareTo(o2);
        }
    }

    public static void main(String[] args) {

        // configuration
        SparkConf conf = new SparkConf().setAppName("Assignment2").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        // input RDD
        JavaRDD<String> input = context.textFile(args[1]);
        input.persist(StorageLevel.MEMORY_AND_DISK());  // TODO

        // RDD that contains information of a node
        // format: nodeName, (value, point-to-iter, route)
        // e.g.: N0, (0, [(N1,4),(N2,3)], "")
        JavaPairRDD<String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>> step1 = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(String s) throws Exception {
                String[] str_arr = s.split(",");
                return new Tuple2<>(str_arr[0], new Tuple2<>(str_arr[1], Integer.parseInt(str_arr[2])));
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>>() {
            @Override
            public Tuple2<String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) throws Exception {
                if (stringIterableTuple2._1.equals(args[0])) {
                    return new Tuple2<>(stringIterableTuple2._1, new Tuple3<>(0, stringIterableTuple2._2, ""));
                }
                return new Tuple2<>(stringIterableTuple2._1, new Tuple3<>(-1, stringIterableTuple2._2, ""));
            }
        });

        // RDD that contains mapped information from RDD step1
        JavaPairRDD<String, Tuple2<Integer, String>> step1_reduce = null;

        // RDD that contains reduced information from RDD step1_reduce
        JavaPairRDD<String, Tuple2<Integer, String>> step1_reduce_result = null;

        // iteration
        // when values of all nodes not changed for continuous 5 iteration steps, safe to quit the iteration
        while(count < 5) {
            if (!isChanged) {
                count++;
            } else {
                count = 0;
            }
            isChanged = false;

            // generate mapped pair
            step1_reduce = step1.filter(new Function<Tuple2<String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>> stringTuple3Tuple2) throws Exception {
                    return (stringTuple3Tuple2._2._1() != -1);
                }
            }).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>>, String, Tuple2<Integer, String>>() {
                @Override
                public Iterator<Tuple2<String, Tuple2<Integer, String>>> call(Tuple2<String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>> stringTuple3Tuple2) throws Exception {
                    ArrayList<Tuple2<String, Tuple2<Integer, String>>> result_list = new ArrayList<>();
                    for (Tuple2<String, Integer> each : stringTuple3Tuple2._2._2()) {
                        result_list.add(new Tuple2<>(each._1, new Tuple2<>(stringTuple3Tuple2._2._1() + each._2, stringTuple3Tuple2._2._3().equals("") ? (stringTuple3Tuple2._1 + "-" + each._1) : (stringTuple3Tuple2._2._3() + "-" + each._1))));
                    }
                    return result_list.iterator();
                }
            });

            // generate the reduced pair
            step1_reduce_result = step1_reduce.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<Integer, String>>>, String, Tuple2<Integer, String>>() {
                @Override
                public Tuple2<String, Tuple2<Integer, String>> call(Tuple2<String, Iterable<Tuple2<Integer, String>>> stringIterableTuple2) throws Exception {
                    // exception for starting node
                    if (stringIterableTuple2._1.equals(args[0])) {
                        return new Tuple2<>(stringIterableTuple2._1, new Tuple2<>(0, ""));
                    }
                    int min_avail = -1;
                    boolean valid = false;
                    String route = "";
                    for (Tuple2<Integer, String> each : stringIterableTuple2._2) {
                        if (each._1 >= 0) {
                            if (valid && each._1 < min_avail) {
                                min_avail = each._1;
                                route = each._2;
                            } else if (! valid) {
                                min_avail = each._1;
                                route = each._2;
                                valid = true;
                            }
                        }
                    }
                    return new Tuple2<>(stringIterableTuple2._1, new Tuple2<>(min_avail, route));
                }
            });

            // combine the reduced pair and the step1
            // note the use of leftOuterJoin to avoid the vanish of nodes
            step1 = step1.leftOuterJoin(step1_reduce_result).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>, Optional<Tuple2<Integer, String>>>>, String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>>() {
                @Override
                public Tuple2<String, Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>> call(Tuple2<String, Tuple2<Tuple3<Integer, Iterable<Tuple2<String, Integer>>, String>, Optional<Tuple2<Integer, String>>>> stringTuple2Tuple2) throws Exception {
                    // check existence
                    if (stringTuple2Tuple2._2._2.isPresent()) {
                        Integer old_value = stringTuple2Tuple2._2._1._1();
                        Integer new_value = stringTuple2Tuple2._2._2.get()._1;
                        String new_route = stringTuple2Tuple2._2._2.get()._2;
                        String result_route = stringTuple2Tuple2._2._1._3();
                        Integer result_value = old_value;
                        // compare the old_value and new_value
                        // -1 or else
                        if (old_value == -1 && new_value >= 0) {
                            isChanged = true;
                            result_value = new_value;
                            result_route = new_route;
                        } else if (old_value != -1 && new_value != -1 && new_value < old_value) {
                            isChanged = true;
                            result_value = new_value;
                            result_route = new_route;
                        }
//                    result_value = ((old_value == -1) || (old_value > 0 && new_value < old_value && new_value >= 0)) ? new_value : old_value;
                        // TODO: need to modify route
                        return new Tuple2<>(stringTuple2Tuple2._1, new Tuple3<>(result_value, stringTuple2Tuple2._2._1._2(), result_route));
                    } else {
                        return new Tuple2<>(stringTuple2Tuple2._1, new Tuple3<>(stringTuple2Tuple2._2._1._1(), stringTuple2Tuple2._2._1._2(), stringTuple2Tuple2._2._1._3()));
                    }
                }
            });
            step1_reduce_result.count();
        }

        // RDD that contains all in-nodes
        JavaPairRDD<String, Integer> input_nodes = input.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(",")[0], 1);
            }
        });

        // RDD that contains all nodes, by union in-nodes and out-nodes and then distinct
        JavaPairRDD<String, Integer> all_nodes = input.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(",")[1], 1);
            }
        }).union(input_nodes).distinct();

        // use rightOuterJoin to include the nodes that have no point-to-edges
        JavaPairRDD<Integer, Tuple2<String, String>> tmp_result = step1_reduce_result.rightOuterJoin(all_nodes).mapToPair(new PairFunction<Tuple2<String, Tuple2<Optional<Tuple2<Integer, String>>, Integer>>, Integer, Tuple2<String, String>>() {
            @Override
            public Tuple2<Integer, Tuple2<String, String>> call(Tuple2<String, Tuple2<Optional<Tuple2<Integer, String>>, Integer>> stringTuple2Tuple2) throws Exception {
                if (stringTuple2Tuple2._2._1.isPresent()) {
                    return new Tuple2<>(stringTuple2Tuple2._2._1.get()._1, new Tuple2<>(stringTuple2Tuple2._1, stringTuple2Tuple2._2._1.get()._2));
                } else {
                    return new Tuple2<>(-1, new Tuple2<>(stringTuple2Tuple2._1, ""));
                }
            }
        });

        // generate the result by filtering the starting node, sorting by key, and mapping to RDD with Node inside
        JavaRDD<Node> result = tmp_result.filter(new Function<Tuple2<Integer, Tuple2<String, String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Tuple2<String, String>> integerTuple2Tuple2) throws Exception {
                return ! integerTuple2Tuple2._2._1.equals(args[0]);
            }
        }).sortByKey(new IntegerComparator()).map(new Function<Tuple2<Integer, Tuple2<String, String>>, Node>() {
            @Override
            public Node call(Tuple2<Integer, Tuple2<String, String>> integerTuple2Tuple2) throws Exception {
                return new Node(integerTuple2Tuple2._2._1, integerTuple2Tuple2._2._2, integerTuple2Tuple2._1);
            }
        }).repartition(1);

        // save the final result
        result.saveAsTextFile(args[2]);

    }
}
