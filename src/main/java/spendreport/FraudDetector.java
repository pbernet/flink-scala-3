package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.*;

/**
 *
 * Business logic of the function that detects fraudulent transactions
 * State and time are considered
 *
 * TODO Fire test data at a more realistic interval now always 200ms waiting (timer is never firing)
 * TODO Switch to com.ververica.models.Transaction (to have full control)
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetector.class);

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;
    private static final long ONE_SECOND = 1000;
    private static final long TH_MILLI_SECONDS = 200;

    // This is a form of "keyed state",
    // meaning it is only available in operators that are applied in a keyed context
    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        LOG.debug("Processing transaction with accountId: " + transaction.getAccountId());

        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();

        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                // Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            cleanUpStates(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // flag the small amount
            flagState.update(true);

            // and set a timer for 1 minute processingTime in the future, see callback onTimer below
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);

            timerState.update(timer);
        }
    }

    //  Clearing the state after 1 minute means that we are essentially
    //  starting over again waiting to detect a pattern
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {

        LocalDateTime date =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        LOG.info("Timer fired at: " + date + ", clear state");
        clearState();
    }

    private void cleanUpStates(Context ctx) throws Exception {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        clearState();
    }
    private void clearState() {
        timerState.clear();
        flagState.clear();
    }
}
