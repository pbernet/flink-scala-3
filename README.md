# Flink API examples for DataStream API and Table API in Scala 3

As of 1.15+ Flink does not expose any specific Scala version anymore.
Users can now choose whatever Scala version they need in their user code, including Scala 3.x.
Note that the [Scala API](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/scala_api_extensions) is not used, since it is deprecated.
[As of 1.16+ there are no feasible alternatives](https://lists.apache.org/thread/7lt5sxrgy4b503z4x2sfq36oy5r7v4vc). Successor candidates are:
* https://github.com/findify/flink-scala-api
* https://github.com/ariskk/flink4s

This repository is a fork of [Flink Scala 3 examples](https://github.com/sjwiesman/flink-scala-3), which is originally based on Timo
Walther's [Flink API Examples for DataStream API and Table API](https://github.com/twalthr/flink-api-examples). You can watch his talk [Flink's Table & DataStream API: A Perfect Symbiosis](https://youtu.be/vLLn5PxF2Lw) on YouTube which walks
through the Java version of this code.
    
Additional Scala examples:
* [TumblingWindows](src/main/scala/com/custom/TumblingWindow.scala)

This Repo contains also some Java examples:
* [FraudDetectionJob](src/main/java/spendreport/FraudDetectionJob.java)
* [WindowWordCount](src/main/java/com/custom/WindowWordCount.java)
  
Other interesting Flink examples: [Immerok examples Repo](https://github.com/pbernet/recipes)

# How to Use This Repository

1. Import this repository into your IDE (preferably IntelliJ IDEA). The project uses the latest Flink version and runs with Java 11.

2. In IDEA `Project structure | Global libraries`  add the corresponding `scala-sdk`

3. All examples are runnable from the IDE or SBT. Execute the `main()` method of every example class.

4. For the Apache Kafka examples, start up Kafka and Zookeeper:

```
cd docker
docker-compose up -d
```

5. Run [FillKafkaWithCustomers](src/main/scala/com/ververica/FillKafkaWithCustomers.scala) and [FillKafkaWithTransactions](src/main/scala/com/ververica/FillKafkaWithTransactions.scala) to create and fill the Kafka topics with data.

6. Monitor the Kafka topics with local [akhq](http://localhost:8081)
