
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinPackState3 {
    //TODO give fup and fdown as paramters to the functions.
    private static final Logger log = LogManager.getLogger(BinPackState3.class);
    public static int size = ConstantsAuto.initialreplicas;//7;//15;//10;//10; // 5; //10;//1; //10;
    //0.5 WSLA is reached around 85 events/sec
    static double wsla = ConstantsAuto.wsla;
    static String action = "none";
    static List<Consumer> assignment = new ArrayList<Consumer>();
    static List<Consumer> currentAssignment = new ArrayList<Consumer>();
    static List<Consumer> tempAssignment = new ArrayList<Consumer>();
    // private static KafkaConsumer<byte[], byte[]> metadataConsumer;

    public static void scaleAsPerBinPack() {
        action = "none";
        log.info("Currently we have this number of consumers group {} {}", "testgroup1", size);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - size;
        if (replicasForscale > 0) {
            action = "up";
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale  group1 by {}", replicasForscale);
            //currentAssignment = assignment;
            return;
        } else {
            int neededsized = binPackAndScaled();
            int replicasForscaled = size - neededsized;
            if (replicasForscaled > 0) {
                action = "down";
                log.info("We have to downscale  group by {} {}", "testgroup1", replicasForscaled);
                return;
            }
        }
        if (assignmentViolatesTheSLA2()) {
            action = "REASS";
        }
        log.info("===================================");
    }


    private static int binPackAndScale() {
        log.info(" shall we upscale group {}", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        // List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);
        List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);
        float fraction = 0.9f;
        for (Partition partition : parts) {

            //TODO change this 200 to ArrivalRates.processingRate
            if (partition.getLag() > ArrivalRates.processingRate * wsla * fraction) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), ArrivalRates.processingRate * wsla * fraction);
                partition.setLag((long) (ArrivalRates.processingRate * wsla * fraction));
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > ArrivalRates.processingRate * fraction) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f", ArrivalRates.processingRate * fraction));
                partition.setArrivalRate(ArrivalRates.processingRate * fraction);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)), (long) (ArrivalRates.processingRate * wsla * fraction),
                        ArrivalRates.processingRate * fraction));
            }

            for (j = 0; j < parts.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {
                    if (consumers.get(i).getRemainingLagCapacity() >= parts.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == parts.size())
                break;
        }
        assignment = consumers;
        tempAssignment = consumers;
        log.info(" The BP up scaler recommended for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }

    static int binPackAndScaled() {
        log.info(" shall we down scale group {} ", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        //List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);
        List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);
        double fractiondynamicAverageMaxConsumptionRate = ArrivalRates.processingRate * 0.2;
        for (Partition partition : parts) {
            if (partition.getLag() > fractiondynamicAverageMaxConsumptionRate * wsla) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                                " we are truncating its lag", partition.getId(), partition.getLag(),
                        fractiondynamicAverageMaxConsumptionRate * wsla);
                partition.setLag((long) (fractiondynamicAverageMaxConsumptionRate * wsla));
            }
        }

        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > fractiondynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f", fractiondynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(fractiondynamicAverageMaxConsumptionRate);
            }
        }
        Collections.sort(parts, Collections.reverseOrder());
        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)),
                        (long) (fractiondynamicAverageMaxConsumptionRate * wsla),
                        fractiondynamicAverageMaxConsumptionRate));
            }

            for (j = 0; j < parts.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {

                    if (consumers.get(i).getRemainingLagCapacity() >= parts.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == parts.size())
                break;
        }
        assignment = consumers;
        log.info(" The BP down scaler recommended  for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }


  /*  private static  boolean assignmentViolatesTheSLA() {
        for (Consumer cons : currentAssignment) {
            if (cons.getRemainingLagCapacity() <  (long) (wsla*200*.9f)||
                    cons.getRemainingArrivalCapacity() < 200f*0.9f){
                return true;
            }
        }
        return false;
    }
*/


    private static boolean assignmentViolatesTheSLA2() {
        //List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);
        List<Partition> partsReset = new ArrayList<>(ArrivalRates.topicpartitions);

        float fraction = ConstantsAuto.fup;
        for (Partition partition : partsReset) {
            if (partition.getLag() > ArrivalRates.processingRate * wsla * fraction) {
                partition.setLag((long) (ArrivalRates.processingRate * wsla * fraction));
            }
        }

        for (Partition partition : partsReset) {
            if (partition.getArrivalRate() > ArrivalRates.processingRate * fraction) {
                partition.setArrivalRate(ArrivalRates.processingRate * fraction);
            }
        }

        for (Consumer cons : currentAssignment) {
            double sumPartitionsArrival = 0;
            double sumPartitionsLag = 0;
            for (Partition p : cons.getAssignedPartitions()) {
                sumPartitionsArrival += partsReset.get(p.getId()).getArrivalRate();
                sumPartitionsLag += partsReset.get(p.getId()).getLag();
            }

            if (sumPartitionsLag > (wsla * ArrivalRates.processingRate * fraction)
                    || sumPartitionsArrival > ArrivalRates.processingRate * fraction) {
                return true;
            }
        }
        return false;
    }

}
