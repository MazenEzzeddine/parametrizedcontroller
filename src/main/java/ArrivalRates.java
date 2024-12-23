import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ArrivalRates {
    private static final Logger log = LogManager.getLogger(ArrivalRates.class);
    static double processingRate = 0;
    static ArrayList<Partition> topicpartitions;

    static {
        topicpartitions = new ArrayList<>();
        for (int i = 0; i < ConstantsAuto.nbpartitions; i++) {
            topicpartitions.add(new Partition(i, 0, 0));
        }
    }
    static  HttpClient client = HttpClient.newHttpClient();
    static void arrivalRateTopic1() throws ExecutionException, InterruptedException {
        ////////////////////////////////////////////////////
        List<URI> partitions = new ArrayList<>();


        for (int i = 0; i < ConstantsAuto.nbpartitions; i++) {
            try {
                partitions.add( new URI(ConstantsAuto.partitionsarrivals[i]));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }

     /*   List<URI> partitionslag = new ArrayList<>();
        for (int i = 0; i < ConstantsAuto.nbpartitions; i++) {
            try {
                partitions.add( new URI(ConstantsAuto.partitionsLag[i]));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }*/
        ///////////////////////////////////////////////////
        //launch queries for topic 1 lag and arrival get them from prometheus
        List<CompletableFuture<String>> partitionsfutures = partitions.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());

/*
        List<CompletableFuture<String>> partitionslagfuture = partitionslag.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());*/


        int partition = 0;
        double totalarrivalstopic1 = 0.0;
        double partitionArrivalRate;
        for (CompletableFuture<String> cf : partitionsfutures) {
            try {
                partitionArrivalRate = Util.parseJsonArrivalRate(cf.get(), partition);
            } catch (Exception e) {
               // e.printStackTrace();
                return;
            }
            topicpartitions.get(partition).setArrivalRate(partitionArrivalRate);
            totalarrivalstopic1 += partitionArrivalRate;
            log.info("arrival rate into partition {} is {}", partition,  partitionArrivalRate);
            partition++;
        }
        log.info("totalArrivalRate for  topic 1 {}", totalarrivalstopic1);

      /*  partition = 0;
        double totallag = 0.0;
        long partitionLag = 0L;
        for (CompletableFuture<String> cf : partitionslagfuture) {
            try {
                partitionLag = Util.parseJsonArrivalLag(cf.get(), partition).longValue();
            } catch (InterruptedException | ExecutionException e) {
                //e.printStackTrace();
                return;
            }
            topicpartitions.get(partition).setLag(partitionLag);
            totallag += partitionLag;
            log.info("lag of partition {} is {}", partition,  partitionLag);
            partition++;
        }
        log.info("totalLag for topic 1 {}", totallag);*/
        queryLatency();
        log.info("******************");
    }



    private static void queryLatency()  {
        List<URI> latencies = new ArrayList<>();
        try {
            latencies = Arrays.asList(
                    new URI(ConstantsAuto.processingQuery)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<CompletableFuture<String>> latenciesFuture = latencies.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());

        int index = 0;
        double lat;

        for (CompletableFuture<String> cf : latenciesFuture) {
            try {
                lat = Util.parseJsonLatency(cf.get());
                if (lat == 0.0 || Double.isNaN(lat)) return;
                if (index == 0) {
                    processingRate = lat;
                    log.info("processing rate  is {}", processingRate);
                } else {
                    // count = lat;
                    //log.info("processing rate 95 percentile over 10s (mu) is {}", processingRate);
                }
                index++; // why is that?
            } catch (Exception e) {
                // e.printStackTrace();
                // log.info("Exception occured")
                return;
            }
        }

    }






}



