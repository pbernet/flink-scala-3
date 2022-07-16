package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;


/**
 * Java implementation of:
 * https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/try-flink/datastream
 * <p>
 * Similar to: [[Example_05_DataStream_Deduplicate.scala]]
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/sources/
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream<Alert> alerts = transactions
                // To ensure that the same *physical* task processes all records for a particular key
                .keyBy(Transaction::getAccountId)
                // Adds an operator that applies a function to each partitioned element in the stream
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts
                // writes a DataStream to an external system
                .addSink(new AlertSink())
                .name("send-alerts");

        env.execute("Fraud Detection");
    }
}
