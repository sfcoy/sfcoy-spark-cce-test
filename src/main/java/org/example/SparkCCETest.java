package org.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.Tuple3;

public class SparkCCETest {

    private static final Logger logger = LoggerFactory.getLogger(SparkCCETest.class);

    public static void main(String[] args) {
        try (final SparkSession sparkSession = SparkSession.builder()
                .appName("ClassCastException Test")
                .getOrCreate();
             final JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext())) {

            final Dataset<Machine> machineRecords = sparkSession
              .read()
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load("src/main/data/machines.csv")
              .as(Encoders.bean(Machine.class))
              .persist();

            final int workerCount = sparkContext.defaultParallelism();
            logger.info("Worker count: {}", workerCount);

            final JavaPairRDD<String, List<Nut>> nutsByMachine = sparkSession
              .read()
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load("src/main/data/nuts.csv")
              .as(Encoders.bean(Nut.class))
              .toJavaRDD()
              .mapToPair(nut -> new Tuple2<>(nut.getMachineType(), nut))
              .repartitionAndSortWithinPartitions(new HashPartitioner(workerCount))
              .combineByKey(SparkCCETest::createListAndCombine, SparkCCETest::mergeValues, SparkCCETest::mergeNutCombiners)
              .persist(StorageLevel.MEMORY_AND_DISK());

            final JavaPairRDD<String, List<Bolt>> boltsByMachine = sparkSession
              .read()
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load("src/main/data/bolts.csv")
              .as(Encoders.bean(Bolt.class))
              .toJavaRDD()
              .mapToPair(bolt -> new Tuple2<>(bolt.getMachineType(), bolt))
              .repartitionAndSortWithinPartitions(new HashPartitioner(workerCount))
              .combineByKey(SparkCCETest::createListAndCombine, SparkCCETest::mergeValues, SparkCCETest::mergeBoltCombiners)
              .persist(StorageLevel.MEMORY_AND_DISK());

            machineRecords
              .toJavaRDD()
              .mapToPair(machine -> new Tuple2<>(machine.getMachineType(), machine))
              .join(nutsByMachine)
              .join(boltsByMachine)
              .map(Tuple2::_2)
              .map(tuples -> new Tuple3<>(tuples._1._1, tuples._1._2, tuples._2))
              .mapToPair(machineWithNutsBolts -> new Tuple2<>(exportFileFor(machineWithNutsBolts._1()), machineWithNutsBolts))
              .repartitionAndSortWithinPartitions(new HashPartitioner(workerCount))
              .foreachPartition(machineIterator -> {
                  while (machineIterator.hasNext()) {
                      final Tuple2<String, Tuple3<Machine, List<Nut>, List<Bolt>>> sectionOptions = machineIterator.next();
                      logger.info("Found {} in partition {}", sectionOptions._1(), TaskContext.getPartitionId());
                  }
              });
        }
    }

    static String exportFileFor(Machine machine) {
        return machine.getId().substring(0, 5);
    }

    static List<Nut> createListAndCombine(Nut v) {
        List<Nut> c = new ArrayList<>();
        c.add(v);
        return c;
    }

    static List<Nut> mergeValues(List<Nut> c, Nut v) {
        c.add(v);
        return c;
    }

    static List<Nut> mergeNutCombiners(List<Nut> c1, List<Nut> c2) {
        c1.addAll(c2);
        return c1;
    }

    static List<Bolt> createListAndCombine(Bolt v) {
        List<Bolt> c = new ArrayList<>();
        c.add(v);
        return c;
    }

    static List<Bolt> mergeValues(List<Bolt> c, Bolt v) {
        c.add(v);
        return c;
    }

    static List<Bolt> mergeBoltCombiners(List<Bolt> c1, List<Bolt> c2) {
        c1.addAll(c2);
        return c1;
    }

}
