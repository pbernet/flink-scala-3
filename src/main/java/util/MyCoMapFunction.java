package util;

import com.ververica.models.Customer;
import com.ververica.models.Transaction;

import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Example of a user defined function
 * Doc:
 * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/user_defined_functions
 */
public class MyCoMapFunction implements CoMapFunction<Customer, Transaction, Object> {
  @Override
  public Customer map1(Customer value) {
    return value;
  }

  @Override
  public Transaction map2(Transaction value) {
    return value;
  }
}
