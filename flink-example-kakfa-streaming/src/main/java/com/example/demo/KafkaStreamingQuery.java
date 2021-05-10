package com.example.demo;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;


/**
Kafka 命令
==========
cd apache-zookeeper-3.6.0-bin\bin
zkServer.cmd

cd kafka_2.12-2.2.0
bin\windows\kafka-server-start.bat config\server.properties


bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic flink-test
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic flink-output-topic
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic flink-side-output-topic

bin\windows\kafka-topics.bat --list --zookeeper localhost:2181
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic flink-test
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic flink-test --from-beginning
*/

/**
Kafka 消息
==========
{"id": "id-001", "time": "2020-01-01 08:00:01", "value": 100}
{"id": "id-002", "time": "2020-01-01 08:00:02", "value": 200}
{"id": "id-003", "time": "2020-01-01 08:00:03", "value": 300}
{"id": "id-001", "time": "2020-01-01 08:17:01", "value": 117}
{"id": "id-002", "time": "2020-01-01 08:17:02", "value": 217}
{"id": "id-003", "time": "2020-01-01 08:17:03", "value": 317}
{"id": "id-001", "time": "2020-01-01 08:02:01", "value": 102}
{"id": "id-002", "time": "2020-01-01 08:02:02", "value": 202}
{"id": "id-003", "time": "2020-01-01 08:02:03", "value": 302}
{"id": "id-001", "time": "2020-01-01 08:03:01", "value": 103}
{"id": "id-002", "time": "2020-01-01 08:03:02", "value": 203}
{"id": "id-003", "time": "2020-01-01 08:03:03", "value": 303}
{"id": "id-001", "time": "2020-01-01 08:20:01", "value": 120}
{"id": "id-002", "time": "2020-01-01 08:20:02", "value": 220}
{"id": "id-003", "time": "2020-01-01 08:20:03", "value": 320}
{"id": "id-001", "time": "2020-01-01 08:05:01", "value": 105}
{"id": "id-002", "time": "2020-01-01 08:05:02", "value": 205}
{"id": "id-003", "time": "2020-01-01 08:05:03", "value": 305}
{"id": "id-001", "time": "2020-01-01 08:30:01", "value": 130}
{"id": "id-002", "time": "2020-01-01 08:30:02", "value": 230}
{"id": "id-003", "time": "2020-01-01 08:30:03", "value": 330}
{"id": "id-001", "time": "2020-01-01 08:47:01", "value": 147}
{"id": "id-002", "time": "2020-01-01 08:47:02", "value": 247}
{"id": "id-003", "time": "2020-01-01 08:47:03", "value": 347}
{"id": "id-001", "time": "2020-01-01 08:32:01", "value": 132}
{"id": "id-002", "time": "2020-01-01 08:32:02", "value": 232}
{"id": "id-003", "time": "2020-01-01 08:32:03", "value": 332}
{"id": "id-001", "time": "2020-01-01 08:33:01", "value": 133}
{"id": "id-002", "time": "2020-01-01 08:33:02", "value": 233}
{"id": "id-003", "time": "2020-01-01 08:33:03", "value": 333}
{"id": "id-001", "time": "2020-01-01 08:50:01", "value": 150}
{"id": "id-002", "time": "2020-01-01 08:50:02", "value": 250}
{"id": "id-003", "time": "2020-01-01 08:50:03", "value": 350}
{"id": "id-001", "time": "2020-01-01 08:35:01", "value": 135}
{"id": "id-002", "time": "2020-01-01 08:35:02", "value": 235}
{"id": "id-003", "time": "2020-01-01 08:35:03", "value": 335}
 */

public class KafkaStreamingQuery {
    
    public void execute(String inputFormat, String sinkType, String windowFunction) {
        
        // *******
        // 验证参数
        // *******
        if (inputFormat == null) {
            inputFormat = "custom";
            System.out.println("inputFormat is null, set to " + inputFormat);
        } else if (inputFormat != "json" && inputFormat != "string"
                && inputFormat != "custom" && inputFormat != "fromElements") {
            System.out.println("invalid input format: " + inputFormat);
            return;
        } else {
            System.out.println("inputFormat is : " + inputFormat);
        }
        
        if (sinkType == null) {
            sinkType = "kafka";
            System.out.println("sinkType is null, set to " + sinkType);
        } else if (sinkType != "kafka" && sinkType != "console") {
            System.out.println("invalid sink type: " + sinkType);
            return;
        } else {
            System.out.println("sinkType is : " + sinkType);
        }
        
        if (windowFunction == null) {
            windowFunction = "AggregateFunction";
            System.out.println("windowFunction is null, set to " + windowFunction);
        } else if (windowFunction != "ReduceFunction" && windowFunction != "AggregateFunction"
                && windowFunction != "ProcessWindowFunction") {
            // FoldFunction 在 1.12 被删除了
            System.out.println("invalid window function: " + windowFunction);
            return;
        } else {
            System.out.println("windowFunction is : " + windowFunction);
        }
        
        // ***************
        // 获取 Stream 环境
        // ***************
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        System.out.println("create ExecutionEnvironment " + env.getClass());
        
        env.enableCheckpointing(10000);   // 打开 checkpoint，默认是关闭的
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 设置一致性，默认就是 EXACTLY_ONCE
        
        // ***********************
        // 设置 TimeCharacteristic
        // ***********************
        
        /*
         * Flink 1.12 之前
         *     默认使用 ProcessingTime，
         *     如果要使用 EventTime 需要调用 setStreamTimeCharacteristic 设置，
         *     并且使用 EventTime 类型的 Window 比如 SlidingEventTimeWindows，
         *     并且在自定义的 Watermark 类里面解析 event 时间，
         *     EventTimeWindow 必然要定义 Watermark 类指定解析 event 时间的函数，哪怕实际并不需要 Watermark，
         *     这种情况可以通过 ExecutionConfig#setAutoWatermarkInterval(0) 关闭 Watermark (默认是 200 ms 计算一次 watermark)
         *     
         * Flink 1.12 开始
         *     默认使用 EventTime
         *     如果要使用 ProcessingTime 不用调 setStreamTimeCharacteristic
         *     只需要使用 ProcessingTime 类型的 Window 比如 SlidingProcessingTimeWindows
         *     ProcessingTime 不需要 watermark 因为 ProcessingTime 不存在乱序的情况 
         *     
         *     Watermark 的设置方式和 1.12 之前的有些不同
         */
        
        /* 
         * env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
         */
        
        
        // ******************************
        // fromElements and State Example
        // ******************************
        if (inputFormat == "fromElements") {
            env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
               .keyBy((tuple) -> tuple.f0)
               .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>(){
                   private static final long serialVersionUID = 6952152937635099457L;
                   
                   // 定义状态变量 ValueState
                   // 此外还有 ListState/ReducingState/AggregatingState/FoldingState/MapState 等状态类
                   private transient ValueState<Tuple2<Long, Long>> sum;

                   @Override
                   public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
                       // 第一次使用状态，设置一个初始值
                       if (sum.value() == null) {
                           sum.update(Tuple2.of(0L, 0L));
                       }
                       
                       // 更新状态
                       Tuple2<Long, Long> currentSum = sum.value();
                       currentSum.f0 += 1;
                       currentSum.f1 += input.f1;
                       sum.update(currentSum);

                       if (currentSum.f0 >= 2) {
                           out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                           // 清理状态
                           sum.clear();
                       }
                   }

                   @Override
                   public void open(Configuration config) {
                       // 初始化状态
                       ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                               new ValueStateDescriptor<>(
                                       "average", // the state name
                                       TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})  // type information
                               );
                       sum = getRuntimeContext().getState(descriptor);
                   }
               })
               .print();
            
            // the printed output will be (1,4) and (1,5)
            // 第一个是 (3+5)/2，第二个是 (7+4)/2，接下来只剩一个不会触发
            // 这个例子都是相同的 key，如果是不同的 key 会按 key 聚合，sum 会自动和 key 关联
            
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            return;
        }
        
        // *****************
        // 设置 Kafka Source
        // *****************        
        
        // earliest 只有在找不到初始 offset 的时候才起作用, 如果设置了 group id, 就不起作用了,
        // 但如果不设 group id 的话那 Flink 会报错
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Flink-Test");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        String topic = "flink-test";
        
        /*
         * FlinkKafkaConsumer09/FlinkKafkaProducer09   - flink-connector-kafka-0.9_2.11  - 0.9.x
         * FlinkKafkaConsumer010/FlinkKafkaProducer010 - flink-connector-kafka-0.10_2.11 - 0.10.x
         * FlinkKafkaConsumer/FlinkKafkaProducer       - flink-connector-kafka_2.11      - >= 1.0.0
         */
        
        /*        
        // 正则表达式 topic
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                java.util.regex.Pattern.compile("test-topic-[0-9]"),
                new SimpleStringSchema(),
                properties);
        */
        
        /*
        // 指定 offset, 这种方法不会因为 Properties 设置了 group id 就不起作用
        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("Flink-Test", 0), 23L);
        specificStartOffsets.put(new KafkaTopicPartition("Flink-Test", 1), 31L);
        specificStartOffsets.put(new KafkaTopicPartition("Flink-Test", 2), 43L);
        kafkaConsumer.setStartFromSpecificOffsets(specificStartOffsets);
        */
        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("flink-test", 0), 0L);
        
        DataStream<KafkaEvent> eventStream;
        
        if (inputFormat == "json") {
            System.out.println("convert kafka message to json node");
            
            // JSONKeyValueDeserializationSchema 保存 Kafka 的 key 和 value，
            // 并且 true 表示还会保存 topic、offset 等 meta 信息
            FlinkKafkaConsumer<ObjectNode> kafkaConsumerObjectNode =
                    new FlinkKafkaConsumer<>(topic, new JSONKeyValueDeserializationSchema(true), properties);
            
            // 强制从指定的 offset 开始读取
            kafkaConsumerObjectNode.setStartFromSpecificOffsets(specificStartOffsets);
            
            DataStream<ObjectNode> rawStream = env.addSource(kafkaConsumerObjectNode);
            
            eventStream = rawStream.map(new MapFunction<ObjectNode, KafkaEvent>() {
                private static final long serialVersionUID = 1L;
                private ObjectMapper objectMapper;

                @Override
                public KafkaEvent map(ObjectNode node) throws Exception {
                    if (objectMapper == null) {
                        objectMapper = new ObjectMapper();
                        
                        // 定义 mapper 解析日期时使用的格式
                        objectMapper.setConfig(
                                objectMapper.getDeserializationConfig().with(
                                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")));
                    }
                    
                    System.out.println("key = " + node.get("key"));
                    System.out.println("value = " + node.get("value"));
                    System.out.println("partition = " + node.get("metadata").get("partition"));
                    System.out.println("offset = " + node.get("metadata").get("offset"));
                    
                    // 由于测试是先发数据再跑程序，导致数据获取的间隔非常短，而 watermark 又是默认 200 ms 触发一次，
                    // 会出现 watermark 触发时所有数据已经拿到的情况，这样就无法测试 side output 的 case
                    // 在这里解析数据的时候休眠 500 ms 保证每个数据到来后都会触发 watermark
                    Thread.sleep(500);
                    
                    return objectMapper.readValue(node.get("value").toString(), KafkaEvent.class);
                }
            });
        } else if (inputFormat == "string") {
            System.out.println("convert kafka message to a string");
            
            FlinkKafkaConsumer<String> kafkaConsumerString =
                    new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

            // 强制从指定的 offset 开始读取
            kafkaConsumerString.setStartFromSpecificOffsets(specificStartOffsets);
            
            DataStream<String> rawStream = env.addSource(kafkaConsumerString);
            
            eventStream = rawStream.map(new MapFunction<String, KafkaEvent>() {
                private static final long serialVersionUID = 1L;
                private ObjectMapper objectMapper;
                
                @Override
                public KafkaEvent map(String value) throws Exception {
                    if (objectMapper == null) {
                        objectMapper = new ObjectMapper();
                        
                        // 定义 mapper 解析日期时使用的格式
                        objectMapper.setConfig(
                                objectMapper.getDeserializationConfig().with(
                                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")));
                    }
                    
                    System.out.println("value = " + value);
                    
                    // 由于测试是先发数据再跑程序，导致数据获取的间隔非常短，而 watermark 又是默认 200 ms 触发一次，
                    // 会出现 watermark 触发时所有数据已经拿到的情况，这样就无法测试 side output 的 case
                    // 在这里解析数据的时候休眠 500 ms 保证每个数据到来后都会触发 watermark
                    Thread.sleep(500);
                            
                    return objectMapper.readValue(value, KafkaEvent.class);
                }
            });
        } else if (inputFormat == "custom") {
            System.out.println("convert kafka message to a custom class");
            
            FlinkKafkaConsumer<KafkaEvent> kafkaConsumerCustom =
                    new FlinkKafkaConsumer<>(topic, new KafkaEventSchema(), properties);
            
            // 强制从指定的 offset 开始读取
            kafkaConsumerCustom.setStartFromSpecificOffsets(specificStartOffsets);
            
            eventStream = env.addSource(kafkaConsumerCustom);
        } else {
            System.out.println("invalid inputFormat: " + inputFormat);
            return;
        }
        
        // ************************
        // 设置 watermark 和 window
        // ************************
        
        // Flink 1.12 之前
        
        /*
        DataStream<KafkaEvent> withTimestampsAndWatermarks = eventStream
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<KafkaEvent>() {
                    // 定义 Watermark，将所有收到的数据所携待的时间的最大值，减去 maxOutOfOrderness 作为 Watermark
                    // 假设窗口大小是 10 分钟，maxOutOfOrderness 是 3 分钟，那么
                    // （0，10）窗口需要在收到的数据时间大于等于 13 的时候才触发
                    private final long maxOutOfOrderness = 180000; // 3 minutes
                    private long currentMaxTimestamp = 0;

                    @Override
                    public long extractTimestamp(KafkaEvent element, long previousElementTimestamp) {
                        // extractTimestamp 函数决定如何获取 Event Time
                        // element 是当前 Event，previousElementTimestamp 是上一个 Event Time
                        long timestamp = element.getTime().getTime();
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        // getCurrentWatermark 被周期性的调用
                        // 如果返回的 Watermark 大于窗口的结束时间，那就触发窗口的计算
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                    
                });
        */
        
        final OutputTag<KafkaEvent> lateOutputTag = new OutputTag<KafkaEvent>("late-data"){
            private static final long serialVersionUID = 1L;
        };
        
        WindowedStream<KafkaEvent, String, TimeWindow> windowedStream = eventStream
                .assignTimestampsAndWatermarks(new WatermarkStrategy<KafkaEvent>() {

                    // WatermarkStrategy 提供一些内置的 watermark
                    // 
                    // 比如 WatermarkStrategy.forBoundedOutOfOrderness 函数返回 BoundedOutOfOrdernessWatermarks
                    // 注意这里的 forBoundedOutOfOrderness 使用了函数式接口的方法实现 createWatermarkGenerator 函数
                    // 使用函数式接口需要在 interface 或继承的 interface 使用 @FunctionalInterface 注解
                    
                    // watermark 的具体调用可以参考 TimestampsAndWatermarksOperator
                    // 
                    // 可以看到首先会获取 watermark strategy 的 timestampAssigner 和 watermarkGenerator
                    // 然后通过 getExecutionConfig().getAutoWatermarkInterval(); 判断要不要启动 timer
                    // 
                    // 每收到一个数据的时候，就会调用 timestampAssigner 获取数据的 event time
                    // collect 该数据
                    // 再调用 watermarkGenerator.onEvent 函数
                    //
                    // 如果有启动 timer，超时后会调用 watermarkGenerator.onPeriodicEmit(wmOutput);
                    // 然后再启动 timer
                    
                    private static final long serialVersionUID = -7830975708105721992L;

                    @Override
                    public WatermarkGenerator<KafkaEvent> createWatermarkGenerator(
                            WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<KafkaEvent>() {
                            
                            // 定义 Watermark，将所有收到的数据所携待的时间的最大值，减去 outOfOrdernessMillis 作为 Watermark
                            // 假设窗口大小是 10 分钟，outOfOrdernessMillis 是 3 分钟，那么
                            // （0，10）窗口需要在收到的数据时间大于等于 13 的时候才触发
                            private long maxTimestamp = 0;
                            private long outOfOrdernessMillis = 180000;

                            @Override
                            public void onEvent(KafkaEvent event, long eventTimestamp, WatermarkOutput output) {
                                System.out.println("eventTimestamp = " + eventTimestamp + ", maxTimestamp = " + maxTimestamp);
                                
                                // onEvent 函数每次收到数据都会调用，eventTimestamp 是当前数据的时间，通过 timestampAssigner 函数获取
                                // 在这里 emit watermark 也是可以的
                                maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {                                
                                if (maxTimestamp == 0) {
                                    return;
                                }
                                
                                System.out.println("emitWatermark " + (maxTimestamp - outOfOrdernessMillis - 1));
                                
                                // onPeriodicEmit 会被周期性调用
                                // 可以通过 ExecutionConfig#setAutoWatermarkInterval() 设置周期，设为 0 就不会调用
                                output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
                            }
                        };
                    }
                    
                    @Override
                    public TimestampAssigner<KafkaEvent> createTimestampAssigner(
                            TimestampAssignerSupplier.Context context) {
                        return new TimestampAssigner<KafkaEvent>() {

                            // timestampAssigner 决定了如何获取 Event Time
                            @Override
                            public long extractTimestamp(KafkaEvent element, long recordTimestamp) {
                                return element.getTime().getTime();
                            }
                        };
                    }
                  // new WatermarkStrategy 后紧跟着 withIdleness 是再封装一层
                  // 表示如果一定时间内没收到新数据，就将这个流标记为空闲，这样下游的 operator 不需要等待水印的到来
                  // 当再次收到新数据时，这个流会重新变成活跃状态
                }.withIdleness(Duration.ofMinutes(10)))
                
                .filter((event) -> event.getValue() > 0)   // FilterFunction 是使用 @FunctionalInterface 注解的函数式接口
                
                .keyBy((event) -> event.getId())           // 按 id 分组，如果不用 keyBy，那后面的窗口函数必须改成 windowAll      
                                                           // keyBy 后如果不接 window，可以调用 filter、map、process 等函数
                
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(10)))   // 指定窗口为 Event Time 滑动窗口
                //.trigger(EventTimeTrigger.create()) // Trigger 决定 window 如何触发
                                                      // SlidingEventTimeWindows 默认就是使用 EventTimeTrigger
                                                      // 此外内置的还有 ProcessingTimeTrigger、CountTrigger、PurgingTrigger
                                                      // 通常都不用指定，使用相应 window 默认的就可以
                
                //.evictor(TimeEvictor.of(Time.of(10, TimeUnit.MINUTES))); // evictor 用于在窗口执行之前或之后按特定要求删除数据
                                                                           // 没有默认 evictor，通常也都不需要指定，除了 GlobalWindows
                
                //.allowedLateness(Time.seconds(30))  // watermark 触发 window 后再等 30 秒，默认是 0
                
                .sideOutputLateData(lateOutputTag);   // 获取 window 结束后才到来的数据
        
        // *****************
        //  处理 window 数据
        // *****************
        
        // 处理 window 数据
        // 单个数据如 map、filter、process 之类的操作要在 window 前执行
        // 或者执行 window 数据后才可以做 map、filter 等操作
        
        SingleOutputStreamOperator<Result> result;
        
        if (windowFunction == "AggregateFunction") {
            result = windowedStream.aggregate(new AggregateFunction<KafkaEvent, Tuple2<Long, Long>, Double>() {
                // AggregateFunction 需要定义三个数据，输入数据类型，中间值类型，输出值类型
                private static final long serialVersionUID = 1L;
    
                @Override
                public Tuple2<Long, Long> createAccumulator() {
                    // 初始化中间值
                    return new Tuple2<>(0L, 0L);
                }
    
                @Override
                public Tuple2<Long, Long> add(KafkaEvent value, Tuple2<Long, Long> accumulator) {
                    // 依据每个输入数据，更新中间值
                    return new Tuple2<>(accumulator.f0 + value.getValue(), accumulator.f1 + 1L);
                }
    
                @Override
                public Double getResult(Tuple2<Long, Long> accumulator) {
                    // 依据中间值，计算输出值，这里计算平均值
                    return ((double) accumulator.f0) / accumulator.f1;
                }
    
                @Override
                public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                    // 合并两个中间值
                    return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                }
                
            }, new ProcessWindowFunction<Double, Result, String, TimeWindow>(){
                // 如果只有 AggregateFunction 那么 windowedStream.aggregate 的最终输出就是 AggregateFunction::getResult 的输出
                // 加上 ProcessWindowFunction 后，getResult 的值会传给 ProcessWindowFunction::process 进一步处理
                // 这里的目的是给 AggregateFunction::getResult 的输出加上 window 信息
                private static final long serialVersionUID = 1L;

                @Override
                public void process(String key, ProcessWindowFunction<Double, Result, String, TimeWindow>.Context context,
                        Iterable<Double> element, Collector<Result> out) throws Exception {
                    
                    Timestamp tsStart = new Timestamp(context.window().getStart());
                    Timestamp tsEnd = new Timestamp(context.window().getEnd()); 
                    
                    DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String startStr = sdf.format(tsStart);
                    String endStr = sdf.format(tsEnd);
                    
                    Result result = new Result();
                    result.setId(key);
                    result.setValue(element.iterator().next());
                    result.setWindowStartTime(startStr);
                    result.setWindowEndTime(endStr);
                    
                    out.collect(result);
                }
            });
        } else if (windowFunction == "ReduceFunction") {
            result = windowedStream.reduce(new ReduceFunction<KafkaEvent>() {
                private static final long serialVersionUID = 1L;

                @Override
                public KafkaEvent reduce(KafkaEvent value1, KafkaEvent value2) throws Exception {
                    // reduce 的输入和输出只能是相同类型，这里计算总和
                    KafkaEvent kafkaEvent = new KafkaEvent();
                    kafkaEvent.setId(value1.getId());
                    kafkaEvent.setValue(value1.getValue() + value2.getValue());
                    return kafkaEvent;
                }
                
            }, new ProcessWindowFunction<KafkaEvent, Result, String, TimeWindow>(){
                // 如果只有 ReduceFunction 那么 windowedStream.reduce 的最终输出就是 ReduceFunction::reduce 的输出
                // 加上 ProcessWindowFunction 后，reduce 的值会传给 ProcessWindowFunction::process 进一步处理
                // 这里的目的是给 ReduceFunction::reduce 的输出加上 window 信息
                
                private static final long serialVersionUID = 1L;

                @Override
                public void process(String key,
                        ProcessWindowFunction<KafkaEvent, Result, String, TimeWindow>.Context context,
                        Iterable<KafkaEvent> element, Collector<Result> out) throws Exception {
                    
                    Timestamp tsStart = new Timestamp(context.window().getStart());
                    Timestamp tsEnd = new Timestamp(context.window().getEnd()); 
                    
                    DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String startStr = sdf.format(tsStart);
                    String endStr = sdf.format(tsEnd);
                    
                    Result result = new Result();
                    result.setId(key);
                    result.setValue(element.iterator().next().getValue().doubleValue());
                    result.setWindowStartTime(startStr);
                    result.setWindowEndTime(endStr);
                    
                    out.collect(result);                    
                }                
            });
        } else if (windowFunction == "ProcessWindowFunction") {
            result = windowedStream.process(new ProcessWindowFunction<KafkaEvent, Result, String, TimeWindow>(){
                // ProcessWindowFunction 比较灵活，可以获取窗口的所有数据，但更耗性能和资源
                private static final long serialVersionUID = 5487291179458193144L;

                @Override
                public void process(String key,
                        ProcessWindowFunction<KafkaEvent, Result, String, TimeWindow>.Context context,
                        Iterable<KafkaEvent> elements, Collector<Result> out) throws Exception {
                    
                    Timestamp tsStart = new Timestamp(context.window().getStart());
                    Timestamp tsEnd = new Timestamp(context.window().getEnd()); 
                    
                    DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String startStr = sdf.format(tsStart);
                    String endStr = sdf.format(tsEnd);
                    
                    long count = 0;
                    long total = 0;
                    for (KafkaEvent kafkaEvent: elements) {
                        count++;
                        total += kafkaEvent.getValue();
                    }
                    
                    Result result = new Result();
                    result.setId(key);
                    result.setValue((double)total/count);
                    result.setWindowStartTime(startStr);
                    result.setWindowEndTime(endStr);

                    out.collect(result);
                }
            });
        } else {
            return;
        }
                
        // *******
        // 输出结果
        // *******
        if (sinkType == "kafka") {
            System.out.println("output to Kafka");
            
            // 正常输出
            Properties outputProperties = new Properties();
            outputProperties.setProperty("bootstrap.servers", "localhost:9092");
        
            result.addSink(new FlinkKafkaProducer<>(
                    "flink-output-topic",
                    new ResultSchema(),
                    outputProperties));
            
            // 超时输出
            Properties sideOutputProperties = new Properties();
            sideOutputProperties.setProperty("bootstrap.servers", "localhost:9092");
            
            DataStream<KafkaEvent> lateStream = result.getSideOutput(lateOutputTag);
            lateStream.addSink(new FlinkKafkaProducer<>(
                    "flink-side-output-topic",
                    new KafkaEventSchema(),
                    sideOutputProperties));
            
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        } else if (sinkType == "console") {
            System.out.println("output to console");
            
            result.print();
            result.getSideOutput(lateOutputTag).print();
            
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("invalid sinkType : " + sinkType);
            return;
        }
    }
    
    
    // inner class 必须是 static 不然会报无法序列化的错误
    public static class KafkaEvent {
        private String id;
        private Date time;
        private Integer value;
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
        public KafkaEvent() {}
        
        public void setId(String id) {
            this.id = id;
        }
        
        public String getId() {
            return this.id;
        }
        
        public void setTime(Date time) {
            this.time = time;
        }
        
        public Date getTime() {
            return this.time;
        }
        
        public void setValue(Integer value) {
            this.value = value;
        }
        
        public Integer getValue() {
            return this.value;
        }
        
        @Override
        public String toString() {
            return "KafkaEvent(id=" + this.id + 
                   ", time=" + this.time + 
                   ", value=" + this.value + 
                   ")";
        }
    }
    
    public static class KafkaEventSchema implements DeserializationSchema<KafkaEvent>, SerializationSchema<KafkaEvent> {

        private static final long serialVersionUID = 5929973422365645403L;
        private ObjectMapper objectMapper;

        public KafkaEventSchema() {}
        
        @Override
        public TypeInformation<KafkaEvent> getProducedType() {
            return TypeInformation.of(KafkaEvent.class);
        }

        @Override
        public byte[] serialize(KafkaEvent element) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
                
                // 定义 mapper 解析日期时使用的格式
                objectMapper.setConfig(
                        objectMapper.getSerializationConfig().with(
                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")));
            }
            
            try {
                return objectMapper.writeValueAsString(element).getBytes();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public KafkaEvent deserialize(byte[] message) throws IOException {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
                
                // 定义 mapper 解析日期时使用的格式
                objectMapper.setConfig(
                        objectMapper.getDeserializationConfig().with(
                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")));
            }
            
            System.out.println("message = " + new String(message));
            
            // 由于测试是先发数据再跑程序，导致数据获取的间隔非常短，而 watermark 又是默认 200 ms 触发一次，
            // 会出现 watermark 触发时所有数据已经拿到的情况，这样就无法测试 side output 的 case
            // 在这里解析数据的时候休眠 500 ms 保证每个数据到来后都会触发 watermark
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            return objectMapper.readValue(new String(message), KafkaEvent.class);
        }

        @Override
        public boolean isEndOfStream(KafkaEvent nextElement) {
            return false;
        }
    }
    
    // inner class 必须是 static 不然会报无法序列化的错误
    public static class Result {
        private String windowStartTime;
        private String windowEndTime;
        private String id;
        private Double value;
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
        public Result() {}
        
        public void setWindowStartTime(String windowStartTime) {
            this.windowStartTime = windowStartTime;
        }
        
        public String getWindowStartTime() {
            return this.windowStartTime;
        }
        
        public void setWindowEndTime(String windowEndTime) {
            this.windowEndTime = windowEndTime;
        }
        
        public String getWindowEndTime() {
            return this.windowEndTime;
        }
        
        public void setId(String id) {
            this.id = id;
        }
        
        public String getId() {
            return this.id;
        }
        
        public void setValue(Double value) {
            this.value = value;
        }
        
        public Double getValue() {
            return this.value;
        }
        
        @Override
        public String toString() {
            return "Result(id=" + this.id + 
                   ", windowStartTime=" + this.windowStartTime + 
                   ", windowEndTime=" + this.windowEndTime + 
                   ", value=" + this.value + 
                   ")";
        }
    }
    
    public static class ResultSchema implements DeserializationSchema<Result>, SerializationSchema<Result> {

        private static final long serialVersionUID = 5929973422365645403L;
        private ObjectMapper objectMapper = new ObjectMapper();

        public ResultSchema() {}
        
        @Override
        public TypeInformation<Result> getProducedType() {
            return TypeInformation.of(Result.class);
        }

        @Override
        public byte[] serialize(Result element) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            
            try {
                return objectMapper.writeValueAsString(element).getBytes();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public Result deserialize(byte[] message) throws IOException {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            
            return objectMapper.readValue(new String(message), Result.class);
        }

        @Override
        public boolean isEndOfStream(Result nextElement) {
            return false;
        }
    }
    
    public static void main(String[] args) throws ParseException {
        /**
         * 注意要在 pom.xml 添加 log 依赖才能看到日志方便调试
         */
        
        /**
         * 解析输入参数
         */
        String inputFormat;
        String sinkType;
        String windowFunction;
        
        Options options = new Options();
        options.addOption("i", "inputFormat", true, "input format: json or string or custom");
        options.addOption("s", "sink", true, "sink type: kafka or console");
        options.addOption("w", "windowFunction", true,
                "window function: ReduceFunction|AggregateFunction|FoldFunction|ProcessWindowFunction");
        
        CommandLineParser parser = new DefaultParser( );
        CommandLine commandLine = parser.parse( options, args );
        
        inputFormat = commandLine.getOptionValue("i");
        sinkType = commandLine.getOptionValue("s");
        windowFunction = commandLine.getOptionValue("w");
       
        /**
         * 启动 Kafka Stream 程序
         */
        KafkaStreamingQuery kafkaStreaming = new KafkaStreamingQuery();
        kafkaStreaming.execute(inputFormat, sinkType, windowFunction);
    }
}
