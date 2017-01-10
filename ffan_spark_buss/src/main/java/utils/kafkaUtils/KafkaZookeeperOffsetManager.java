package utils.kafkaUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;

import java.net.URI;
import java.security.InvalidParameterException;
import java.util.*;

/**
 * Created by weishuxiao on 16/8/26.
 */
public class KafkaZookeeperOffsetManager {
    private static Logger log = Logger.getLogger(KafkaZookeeperOffsetManager.class);

    private static OffsetResponse getOffsetResponse(SimpleConsumer consumer,
                                                    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo) {
        for (int i = 0; i < 5; i++) {
            try {
                OffsetResponse offsetResponse =
                        consumer.getOffsetsBefore(new OffsetRequest(offsetInfo, kafka.api.OffsetRequest.CurrentVersion(), kafka.api.OffsetRequest.DefaultClientId()));
//                不明异常
//                if (offsetResponse.hasError()) {
//                    throw new RuntimeException("offsetReponse has error.");
//                }
                return offsetResponse;
            } catch (Exception e) {
                log.warn("Fetching offset from leader " + consumer.host() + ":" + consumer.port() + " has failed " + (i + 1)
                        + " time(s). Reason: " + e.getMessage() + " " + (5 - i - 1) + " retries left.");
                if (i < 5 - 1) {
                    try {
                        Thread.sleep((long) (Math.random() * (i + 1) * 1000));
                    } catch (InterruptedException e1) {
                        log.error("Caught interrupted exception between retries of getting latest offsets. " + e1.getMessage());
                    }
                }
            }
        }
        return null;
    }

    private static Map<TopicAndPartition, Long> fetchLatestOffset(HashMap<KafkaLeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo, long offsetSymbol) {
        Map<TopicAndPartition, Long> offsetManger = new HashMap<>();
        Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        for (KafkaLeaderInfo leader : offsetRequestInfo.keySet()) {
            SimpleConsumer consumer = createSimpleConsumer(leader.getUri().getHost(), leader.getUri().getPort());
            // offsetSymbol = -1 Latest Offset
            // offsetSymbol = -2 Earliest Offset
            PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(offsetSymbol, 1);
            ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
            for (TopicAndPartition topicAndPartition : topicAndPartitions) {
                latestOffsetInfo.put(topicAndPartition, partitionLatestOffsetRequestInfo);
            }
            OffsetResponse latestOffsetResponse = getOffsetResponse(consumer, latestOffsetInfo);
            for (TopicAndPartition topicAndPartition : topicAndPartitions) {
                long[] longs = latestOffsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition());
                offsetManger.put(topicAndPartition, longs[0]);
            }
            consumer.close();
        }
        return offsetManger;
    }

    public static Map<TopicAndPartition, Long> getOffsetInfo(String brokerListString, List<String> metaRequestTopics, long offsetSymbol) throws Exception {
        HashMap<KafkaLeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo =
                new HashMap<>();
        List<TopicMetadata> topicMetadataList = getKafkaMetadata(brokerListString, metaRequestTopics);
        for (TopicMetadata topicMetadata : topicMetadataList) {
            for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                // We only care about LeaderNotAvailableCode error on partitionMetadata level
                // Error codes such as ReplicaNotAvailableCode should not stop us.
                partitionMetadata = refreshPartitionMetadataOnLeaderNotAvailable(brokerListString, partitionMetadata, topicMetadata, 3);
                if (partitionMetadata.errorCode() == ErrorMapping.LeaderNotAvailableCode()) {
                    log.info("Skipping the creation of ETL request for Topic : " + topicMetadata.topic()
                            + " and Partition : " + partitionMetadata.partitionId() + " Exception : "
                            + ErrorMapping.exceptionFor(partitionMetadata.errorCode()));
                } else {
                    if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
                        log.warn("Receiving non-fatal error code, Continuing the creation of ETL request for Topic : "
                                + topicMetadata.topic() + " and Partition : " + partitionMetadata.partitionId() + " Exception : "
                                + ErrorMapping.exceptionFor(partitionMetadata.errorCode()));
                    }
                    KafkaLeaderInfo leader =
                            new KafkaLeaderInfo(new URI("tcp://" + partitionMetadata.leader().connectionString()),
                                    partitionMetadata.leader().id());
                    if (offsetRequestInfo.containsKey(leader)) {
                        ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
                        topicAndPartitions.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId()));
                        offsetRequestInfo.put(leader, topicAndPartitions);
                    } else {
                        ArrayList<TopicAndPartition> topicAndPartitions = new ArrayList<>();
                        topicAndPartitions.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId()));
                        offsetRequestInfo.put(leader, topicAndPartitions);
                    }

                }
            }
        }
        return fetchLatestOffset(offsetRequestInfo, offsetSymbol);
    }

    private static PartitionMetadata refreshPartitionMetadataOnLeaderNotAvailable(String brokerListString, PartitionMetadata partitionMetadata,
                                                                                  TopicMetadata topicMetadata, int numTries) {
        int tryCounter = 0;
        while (tryCounter < numTries && partitionMetadata.errorCode() == ErrorMapping.LeaderNotAvailableCode()) {
            log.info("Retry to referesh the topicMetadata on LeaderNotAvailable...");
            List<TopicMetadata> topicMetadataList = getKafkaMetadata(brokerListString, Collections.singletonList(topicMetadata.topic()));
            if (topicMetadataList == null || topicMetadataList.size() == 0) {
                log.warn("The topicMetadataList for topic " + topicMetadata.topic() + " is empty.");
            } else {
                topicMetadata = topicMetadataList.get(0);
                boolean partitionFound = false;
                for (PartitionMetadata metadataPerPartition : topicMetadata.partitionsMetadata()) {
                    if (metadataPerPartition.partitionId() == partitionMetadata.partitionId()) {
                        partitionFound = true;
                        if (metadataPerPartition.errorCode() != ErrorMapping.LeaderNotAvailableCode()) {
                            return metadataPerPartition;
                        } else { //retry again.
                            if (tryCounter < numTries - 1) {
                                try {
                                    Thread.sleep((long) (Math.random() * (tryCounter + 1) * 2000));
                                } catch (Exception ex) {
                                    log.warn("Caught InterruptedException: " + ex);
                                }
                            }
                            break;
                        }
                    }
                }
                if (!partitionFound) {
                    log.error("No matching partition found in the topicMetadata for Partition: "
                            + partitionMetadata.partitionId());
                }
            }
            tryCounter++;
        }
        return partitionMetadata;
    }

    public static List<TopicMetadata> getKafkaMetadata(String brokerListString, List<String> metaRequestTopics) {
        List<String> brokers = Arrays.asList(brokerListString.split("\\s*,\\s*"));
        Collections.shuffle(brokers);
        boolean fetchMetaDataSucceeded = false;
        int i = 0;
        List<TopicMetadata> topicMetadataList = null;
        while (i < brokers.size() && !fetchMetaDataSucceeded) {
            SimpleConsumer consumer = createBrokerConsumer(brokers.get(i));
            try {
                for (int iter = 0; iter < 3; iter++) {
                    try {
                        topicMetadataList = consumer.send(new TopicMetadataRequest(metaRequestTopics)).topicsMetadata();
                        log.info(String.format("Fetching metadata from broker %s with client id %s for %d topic(s) %s", brokers.get(i),
                                consumer.clientId(), metaRequestTopics.size(), metaRequestTopics));
                        fetchMetaDataSucceeded = true;
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.warn(String.format(
                                "Fetching topic metadata with client id %s for topics [%s] from broker [%s] failed, iter[%s]",
                                consumer.clientId(), metaRequestTopics, brokers.get(i), iter));
                        try {
                            Thread.sleep((long) (Math.random() * (iter + 1) * 1000));
                        } catch (InterruptedException ex) {
                            log.warn("Caught InterruptedException: " + ex);
                        }
                    }
                }
            } finally {
                consumer.close();
                i++;
            }
        }
        return topicMetadataList;
    }


    private static SimpleConsumer createBrokerConsumer(String broker) {
        if (!broker.matches(".+:\\d+"))
            throw new InvalidParameterException("The kakfa broker " + broker + " must follow address:port pattern");
        String[] hostPort = broker.split(":");
        return createSimpleConsumer(hostPort[0], Integer.valueOf(hostPort[1]));
    }

    private static SimpleConsumer createSimpleConsumer(String host, int port) {
        SimpleConsumer consumer =
                new SimpleConsumer(host, port, 2000, 2000, kafka.api.OffsetRequest.DefaultClientId());
        return consumer;
    }

    public static Map<TopicAndPartition, Long> getTopicAndPartition(String offsetInfo) {
        if (offsetInfo == null || offsetInfo.isEmpty())
            return null;
        Map<TopicAndPartition, Long> offsetManger = new HashMap<>();
        JSONArray jsonArray;
        try {
            jsonArray=JSON.parseArray(offsetInfo);
        }catch (Exception e){
            return null;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            TopicAndPartition topicAndPartition = new TopicAndPartition(jsonObject.getString("topic"), jsonObject.getInteger("partitions"));
            Long offset = jsonObject.getLong("fromOffset");
            offsetManger.put(topicAndPartition, offset);
        }
        return offsetManger;
    }

    public static boolean isOutOfRange(Map<TopicAndPartition, Long> offsetManger,
                                       Map<TopicAndPartition, Long> earliestOffset) {
        for (TopicAndPartition topicAndPartition : offsetManger.keySet()) {
            long currentOffset = offsetManger.get(topicAndPartition);
            long smallOffset = earliestOffset.get(topicAndPartition);
            if (currentOffset < smallOffset) {
                return true;
            }
        }
        return false;
    }

}
