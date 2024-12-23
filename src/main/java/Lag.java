import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Lag {
    private static final Logger log = LogManager.getLogger(Lag.class);
    public static AdminClient admin = null;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;
    static long totalLag;
    //////////////////////////////////////////////////////////////////////////////
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;



    public  static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantsAuto.bootstrapservers);
        admin = AdminClient.create(props);

    }


    public static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(ConstantsAuto.consumergroup)
                .partitionsToOffsetAndMetadata().get();
        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        for (int i = 0; i < ConstantsAuto.nbpartitions; i++) {
            requestLatestOffsets.put(new TopicPartition(ConstantsAuto.topic, i), OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
         totalLag=0L;
        for (int i = 0; i < ConstantsAuto.nbpartitions; i++) {
            TopicPartition t = new TopicPartition(ConstantsAuto.topic, i);
            long latestOffset = latestOffsets.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();
            //partitions.get(i).setLag(latestOffset - committedoffset);
           //ArrivalProducer.topicpartitions.get(i).setLag(latestOffset-committedoffset);
            ArrivalRates.topicpartitions.get(i).setLag(latestOffset-committedoffset);
            totalLag += ArrivalRates.topicpartitions.get(i).getLag();
            log.info("partition {} has lag {}", i, ArrivalRates.topicpartitions.get(i).getLag());
        }
        log.info("total lag {}", totalLag);
    }




   public  static int queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(ConstantsAuto.consumergroup));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
        int members = consumerGroupDescriptionMap.get(ConstantsAuto.consumergroup).members().size();

        log.info("consumers nb as per kafka {}", members );
        return members;
    }

    public  static ConsumerGroupState queryConsumerGroupState() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(ConstantsAuto.consumergroup));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
           ConsumerGroupState state =  consumerGroupDescriptionMap.get(ConstantsAuto.consumergroup).state();

        log.info("consumer group state  {}", state );
        return state;
    }

}
