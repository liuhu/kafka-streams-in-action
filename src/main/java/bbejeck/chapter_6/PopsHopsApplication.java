package bbejeck.chapter_6;


import bbejeck.chapter_6.processor.BeerPurchaseProcessor;
import bbejeck.chapter_6.processor.KStreamPrinter;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.BeerPurchase;
import bbejeck.util.Topics;
import bbejeck.util.serializer.JsonDeserializer;
import bbejeck.util.serializer.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.UsePreviousTimeOnInvalidTimestamp;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;

public class PopsHopsApplication {


    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Deserializer<BeerPurchase> beerPurchaseDeserializer = new JsonDeserializer<>(BeerPurchase.class);
        Serde<String> stringSerde = Serdes.String();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Serializer<BeerPurchase> beerPurchaseSerializer = new JsonSerializer<>();

        Topology toplogy = new Topology();

        String domesticSalesSink = "domestic-beer-sales";
        String internationalSalesSink = "international-beer-sales";

        // 消息源， 啤酒销售
        String purchaseSourceNodeName = "beer-purchase-source";
        String purchaseProcessor = "purchase-processor";

        // 处理器，根据销售信息的货币类型，分类两个流，
        BeerPurchaseProcessor beerProcessor = new BeerPurchaseProcessor(domesticSalesSink, internationalSalesSink);

        // 添加源
        toplogy.addSource(LATEST, // 消费模式
                          purchaseSourceNodeName,  // 节点名称
                          new UsePreviousTimeOnInvalidTimestamp(), // 时间戳提取器
                          stringDeserializer, // key deserializer
                          beerPurchaseDeserializer, // value deserializer
                          Topics.POPS_HOPS_PURCHASES.topicName())  // 源的 topic


                .addProcessor(purchaseProcessor,  // 节点名称
                              () -> beerProcessor, // 处理器逻辑
                              purchaseSourceNodeName); // 父节点的处理器名称


        //Uncomment these two lines and comment out the printer lines for writing to topics
        // 增加接收器
        toplogy.addSink(
                internationalSalesSink + "Sink", // 接收器的名称
                "international-sales", // 输出到的topic
                stringSerializer, // key 序列化
                beerPurchaseSerializer, // value 序列化
                purchaseProcessor); // 接收器的父节点

        toplogy.addSink(
                domesticSalesSink + "Sink",
                "domestic-sales",
                stringSerializer,
                beerPurchaseSerializer,
                purchaseProcessor);

        //You'll have to comment these lines out if you want to write to topics as they have the same node names
        toplogy.addProcessor(domesticSalesSink,  // 节点名称
                            new KStreamPrinter("domestic-sales"), // 处理器逻辑
                            purchaseProcessor );  // 父节点的处理器名称

        toplogy.addProcessor(internationalSalesSink,  // 节点名称
                             new KStreamPrinter("international-sales"), // 处理器逻辑
                             purchaseProcessor );  // 父节点的处理器名称

        KafkaStreams kafkaStreams = new KafkaStreams(toplogy, streamsConfig);
        MockDataProducer.produceBeerPurchases(5);
        System.out.println("Starting Pops-Hops Application now");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(70000);
        System.out.println("Shutting down Pops-Hops Application  now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "beer-app-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "beer-app-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "beer-app-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.1.119:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
