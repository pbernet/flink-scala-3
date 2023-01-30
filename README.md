# Flink API examples for DataStream API and Table API in Scala 3

As of 1.15.x Flink does not expose any specific Scala version anymore.
Users can now choose whatever Scala version they need in their user code, including Scala 3.x.
Note that the [Scala API](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/scala_api_extensions) is not used, since it is deprecated.

This repository is a fork of the reimplementation of Timo
Walther's [Flink API Examples for DataStream API and Table API](https://github.com/twalthr/flink-api-examples)
examples in Scala 3. You can watch his talk [Flink's Table & DataStream API: A Perfect Symbiosis](https://youtu.be/vLLn5PxF2Lw) on YouTube which walks
through the Java version of this code.
There are Java examples as well eg [FraudDetector](src/main/java/spendreport/FraudDetectionJob.java) 

# How to Use This Repository

1. Import this repository into your IDE (preferably IntelliJ IDEA). The project uses the latest Flink version and runs with Java 11.

2. In IDEA `Project structure | Global libraries`  add the corresponding `scala-sdk`

3. All examples are runnable from the IDE or SBT. Execute the `main()` method of every example class.

4. In order to make the examples run within IntelliJ IDEA, it is necessary to tick
   the `Add dependencies with "provided" scope to classpath` option in the run configuration under `Modify options`.

5. For the Apache Kafka examples, start up Kafka and Zookeeper:

```
cd docker
docker-compose up -d
```

5. Run `FillKafkaWithCustomers` and `FillKafkaWithTransactions` to create and fill the Kafka topics with data.

6. Monitor Kafka topics with local [akhq](http://localhost:8081)