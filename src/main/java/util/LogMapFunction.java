package util;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogMapFunction<A> implements MapFunction<A, A> {
  private static final Logger LOG = LoggerFactory.getLogger(LogMapFunction.class);
  
  @Override
  public A map(A value) {
    LOG.info("Element value: {}", value);
    return value; }
}
