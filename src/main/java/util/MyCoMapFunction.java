package util;

import com.ververica.models.Customer;
import com.ververica.models.Transaction;

import org.apache.flink.streaming.api.functions.co.CoMapFunction;

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
