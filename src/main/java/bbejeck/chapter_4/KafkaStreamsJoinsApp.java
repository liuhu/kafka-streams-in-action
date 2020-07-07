/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bbejeck.chapter_4;

import bbejeck.chapter_4.joiner.PurchaseJoiner;
import bbejeck.chapter_4.timestamp_extractor.TransactionTimestampExtractor;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.CorrelatedPurchase;
import bbejeck.model.Purchase;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


@SuppressWarnings("unchecked")
public class KafkaStreamsJoinsApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsJoinsApp.class);

    /**
     * P94 连接流
     * 流A、流B 通过相同的键，连接流，获得更加丰富的流信息
     * @see ValueJoiner - 新值生成
     * @see JoinWindows - 流 join 时间窗口
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        StreamsBuilder builder = new StreamsBuilder();

        // 序列化定义
        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        // 将原始流 生成新定 key value
        // key -> customerId
        // value -> Purchase mask credit card
        KeyValueMapper<String, Purchase, KeyValue<String,Purchase>> custIdCCMasking = (k, v) -> {
            Purchase masked = Purchase.builder(v).maskCreditCard().build();
            return new KeyValue<>(masked.getCustomerId(), masked);
        };

        // 分裂流的 Predicate 实例定义
        Predicate<String, Purchase> coffeePurchase = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> electronicPurchase = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        int COFFEE_PURCHASE = 0;
        int ELECTRONICS_PURCHASE = 1;

        // 创建流
        KStream<String, Purchase> transactionStream = builder
                .stream( "transactions", Consumed.with(Serdes.String(), purchaseSerde))
                // ⚠️ 这里用的是 map 操作, 可以定义新的 key 和 value，
                // 而 mapValues 只针对 value 的操作
                // 另外 selectKey 只针对 key 的操作
                .map(custIdCCMasking);

        // 分裂流
        KStream<String, Purchase>[] branchesStream = transactionStream.selectKey((k,v)-> v.getCustomerId()).branch(coffeePurchase, electronicPurchase);

        KStream<String, Purchase> coffeeStream = branchesStream[COFFEE_PURCHASE];
        KStream<String, Purchase> electronicsStream = branchesStream[ELECTRONICS_PURCHASE];

        // 用于连接 value， 所以这里定义的是将两个 value 生成 新的 value
        ValueJoiner <Purchase, Purchase, CorrelatedPurchase> purchaseJoiner
                = new PurchaseJoiner(); // 这里 value 合并的实现, 通过 customerId 合并

        // JoinWindows.of -- 连接的两个值之间最大时间差为 20 min
        // JoinWindows.before(5000) -- 被 join 的 StreamB 时间戳最多比 StreamA 时间滞后5秒
        // JoinWindows.after(5000) -- 被 join 的 StreamB 时间戳最多比 StreamA 时间早5秒
        JoinWindows twentyMinuteWindow =  JoinWindows.of(60 * 1000 * 20);

        Joined<String, Purchase, Purchase> joined =
                // '提供' 流的 key、value 的 Serde 和 第二个流 value 的 Serde
                // 值提供一个 key 的 Serde 是因为 两个流的 key 必须是相同类型
                Joined.with(stringSerde, purchaseSerde, purchaseSerde);

        // 合并流， 查看 page 100
        // join 方法会自动触发两个流的自动重新分区
        KStream<String, CorrelatedPurchase> joinedKStream = coffeeStream.join(electronicsStream,
                                                                              purchaseJoiner,
                                                                              twentyMinuteWindow, joined);

        joinedKStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("joined KStream"));

        // used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData(1,1,1);
        
        LOG.info("Starting Join Examples");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Join Examples now");
        kafkaStreams.close();
        MockDataProducer.shutdown();


    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_application");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.1.119:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        // 默认流时间戳获取方法
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
        return props;
    }


}
