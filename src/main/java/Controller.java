import org.apache.kafka.common.ConsumerGroupState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;

public class Controller implements Runnable {
    private static final Logger log = LogManager.getLogger(Controller.class);







    private static void initialize() throws InterruptedException, ExecutionException {

        ConstantsAuto.init();
        ConstantsAuto.printParameters();
        ConstantsAuto.initPrometheusQueries();
        Lag.readEnvAndCrateAdminClient();


        log.info("Warming  {}  minutes", ConstantsAuto.WarmupTime/(60*1000) );
        Thread.sleep(ConstantsAuto.WarmupTime);
        while (true) {
            log.info("Querying Prometheus");
          //  ArrivalProducer.callForArrivals();
            ArrivalRates.arrivalRateTopic1();
            Lag.getCommittedLatestOffsetsAndLag();
            log.info("--------------------");
            log.info("--------------------");
            if(ArrivalRates.processingRate != 0) {
                scaleLogicTail3();
            }

            log.info("Sleeping for {} seconds", ConstantsAuto.decisionInterval);
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(ConstantsAuto.decisionInterval);
        }
    }









    private static void scaleLogicTail3() throws InterruptedException, ExecutionException {

        if (Lag.queryConsumerGroup() != BinPackState3.size  ||
                Lag.queryConsumerGroupState() != ConsumerGroupState.STABLE)  {
            log.info("no action, previous action is not seen yet");
            return;
        }

        BinPackState3.scaleAsPerBinPack();
        if (BinPackState3.action.equals("up") || BinPackState3.action.equals("down")
                || BinPackState3.action.equals("REASS")) {
            BinPackLag3.scaleAsPerBinPack();
        }
    }


    @Override
    public void run() {
        try {
            initialize();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
