import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

//to be removed
public class ArrivalProducer {

    private static final Logger log = LogManager.getLogger(ArrivalProducer.class);
    static double totalArrivalrate;


    public static void callForArrivals() {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("arrivalservice",
                        5002)
                .usePlaintext()
                .build();
        ArrivalServiceGrpc.ArrivalServiceBlockingStub arrivalServiceBlockingStub =
                ArrivalServiceGrpc.newBlockingStub(managedChannel);
        ArrivalRequest request = ArrivalRequest.newBuilder()
                .setArrivalrequest("Give me the arrival rate plz").build();
        ArrivalResponse reply = arrivalServiceBlockingStub.arrivalRate(request);
        log.info("Arrival from the producer is {}", reply);
        totalArrivalrate = reply.getArrival();
        double partitionArrival = reply.getArrival() / ConstantsAuto.nbpartitions;
        log.info("Arrival into each partition is {}", partitionArrival);
        for (int i = 0; i < ConstantsAuto.nbpartitions; i++) {
           ArrivalRates.topicpartitions.get(i).setArrivalRate(partitionArrival);
        }
        managedChannel.shutdown();
        ArrivalRates.queryLatency();

    }










/*    public static void callForConsumers() {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("rateservice", 5002)
                .usePlaintext()
                .build();

        ArrivalServiceGrpc.ArrivalServiceBlockingStub rateServiceBlockingStub
                 =  ArrivalServiceGrpc.newBlockingStub(managedChannel);
        RateRequest request = RateRequest.newBuilder().setRaterequest("Give me the Assignment plz").build();
        RateResponse reply = rateServiceBlockingStub.consumptionRatee(request);
        log.info("latency is {}", reply);
        managedChannel.shutdown();

    }*/


}
