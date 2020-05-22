Spark ClassCastException Reproducer
===

This is a contrived example of an issue I have found in Apache Spark 2.4.5 as well as all 3.0 pre-releases and RCs to date.

The input data consists of three tables:

Machines

| Id | MachineType |
|----|-------------|
| 100001 | A |
| 100002 | B |
| ... | ... |

Nuts

| MachineType | Description |
|-------------|-------------|
| A | M5 |
| A | M6 |
| B | 1/4" |

Bolts

| MachineType | Description |
|-------------|-------------|
| B | 2" x 1/4" |
| A | 20 x M5 |
| B | 1" x 1/8" |

The objective of the code is to create a a list of machines, together with all of their nuts and bolts listed on each line:

100001, M5, M6, 20 x M5
100002, 1/4",  2" x 1/4", 1" x 1/8"
...

Run the code
--

    $SPARK_HOME/bin/spark-submit --master local[4] target/spark-cce-test.jar
