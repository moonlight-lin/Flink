package com.example.demo;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
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


{"id": "id-001", "time": "2020-01-01 08:00:11", "event": "e-100"}
{"id": "id-002", "time": "2020-01-01 08:00:12", "event": "e-200"}
{"id": "id-003", "time": "2020-01-01 08:00:33", "event": "e-300"}
{"id": "id-001", "time": "2020-01-01 08:17:11", "event": "e-117"}
{"id": "id-002", "time": "2020-01-01 08:17:12", "event": "e-217"}
{"id": "id-003", "time": "2020-01-01 08:17:33", "event": "e-317"}
{"id": "id-001", "time": "2020-01-01 08:02:11", "event": "e-102"}
{"id": "id-002", "time": "2020-01-01 08:02:12", "event": "e-202"}
{"id": "id-003", "time": "2020-01-01 08:02:33", "event": "e-302"}
{"id": "id-001", "time": "2020-01-01 08:03:11", "event": "e-103"}
{"id": "id-002", "time": "2020-01-01 08:03:12", "event": "e-203"}
{"id": "id-003", "time": "2020-01-01 08:03:33", "event": "e-303"}
{"id": "id-001", "time": "2020-01-01 08:20:11", "event": "e-120"}
{"id": "id-002", "time": "2020-01-01 08:20:12", "event": "e-220"}
{"id": "id-003", "time": "2020-01-01 08:20:33", "event": "e-320"}
{"id": "id-001", "time": "2020-01-01 08:05:11", "event": "e-105"}
{"id": "id-002", "time": "2020-01-01 08:05:12", "event": "e-205"}
{"id": "id-003", "time": "2020-01-01 08:05:33", "event": "e-305"}
{"id": "id-001", "time": "2020-01-01 08:30:11", "event": "e-130"}
{"id": "id-002", "time": "2020-01-01 08:30:12", "event": "e-230"}
{"id": "id-003", "time": "2020-01-01 08:30:33", "event": "e-330"}
{"id": "id-001", "time": "2020-01-01 08:47:11", "event": "e-147"}
{"id": "id-002", "time": "2020-01-01 08:47:12", "event": "e-247"}
{"id": "id-003", "time": "2020-01-01 08:47:33", "event": "e-347"}

 */


public class KafkaStreamingJoinStreaming {
    
    public void execute(String sinkType, String joinType) {
        
        // *******
        // 验证参数
        // *******
        if (sinkType == null) {
            sinkType = "kafka";
            System.out.println("sinkType is null, set to " + sinkType);
        } else if (sinkType != "kafka" && sinkType != "console") {
            System.out.println("invalid sink type: " + sinkType);
            return;
        } else {
            System.out.println("sinkType is : " + sinkType);
        }
        
        if (joinType == null) {
            joinType = "intervalJoin";
            System.out.println("joinType is null, set to " + joinType);
        } else if (joinType != "windowJoin" && joinType != "intervalJoin") {
            System.out.println("invalid join type: " + joinType);
            return;
        } else {
            System.out.println("joinType is : " + joinType);
        }
        
        // ***************
        // 获取 Stream 环境
        // ***************
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        System.out.println("create ExecutionEnvironment " + env.getClass());
        
        env.enableCheckpointing(10000);   // 打开 checkpoint，默认是关闭的
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 设置一致性，默认就是 EXACTLY_ONCE
        
        // *****************
        // 设置 Kafka Source
        // *****************
            
        DataStream<KafkaValue> kafkaStream_1 = env.addSource(
                getKafkaConsumer("flink-test-1", new KafkaSchema<KafkaValue>(KafkaValue.class))
                );
        
        DataStream<KafkaEvent> kafkaStream_2 = env.addSource(
                getKafkaConsumer("flink-test-2", new KafkaSchema<KafkaEvent>(KafkaEvent.class))
                );

        // **************
        // 设置 watermark
        // **************
        
        SingleOutputStreamOperator<KafkaValue> watermarkStream_1 = kafkaStream_1
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy<KafkaValue>())
                .filter((event) -> event.getValue() > 0);
        
        SingleOutputStreamOperator<KafkaEvent> watermarkStream_2 = kafkaStream_2
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy<KafkaEvent>())
                .filter((event) -> event.getEvent() != null);
        
        // ******
        //  Join
        // ******
        DataStream<Result> result;
        
        if (joinType == "windowJoin") {
            // 这种 join 操作，会将窗口内的数据，两两传到 join 函数处理，
            // 比如两个 stream 在窗口 (08:00 ~ 08:15) 各 10 个数据，那么 join 函数执行 100 次
            // watermark 的处理一个流的 window 操作一样，只是需要两个流的 watermark 都触发了，才开始 join 操作
            
            result = watermarkStream_1
                    .join(watermarkStream_2)
                    .where(value -> value.getId())
                    .equalTo(event -> event.getId())
                    .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(10)))
                    .apply(new JoinFunction<KafkaValue, KafkaEvent, Result> (){
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Result join(KafkaValue first, KafkaEvent second) {                        
                            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        
                            Result result = new Result();
                        
                            result.setValueId(first.getId());
                            result.setValueTime(sdf.format(first.getTime()));
                            result.setValue(first.getValue().doubleValue());
                        
                            result.setEventId(second.getId());
                            result.setEventTime(sdf.format(second.getTime()));
                            result.setEvent(second.getEvent());
                        
                            return result;
                        }
                    });
        } else if (joinType == "intervalJoin") {
            // 这种 join 操作不需要定义窗口，
            // 对于第一个 stream 的每个数据 a，都会寻找第二个 stream 内的时间戳和 a 的差在 between(lowerBound, upperBound) 范围内的数据,
            // 将这些数据分别和 a 组合传给 join 函数,
            // 
            // watermark 依然由 MyWatermarkStrategy 决定，但这个 watermark 不比较窗口，
            // 而是每个数据都和 watermark 比较，决定会不会丢弃，
            // 比如 outOfOrdernessMillis 是 3 分钟，当前所有收到数据的最大时间戳是 08:15，
            // 那么新到来的数据如果时间戳小于 08:12 就会被丢弃，
            //
            // 另外，由于数据需要缓存，以等待另一个流的数据，这个等待时间由 watermark 和 upperBound 决定，
            // 当 watermark 超过数据时间 + upperBound 时，数据就从缓存删除，
            // 比如 outOfOrdernessMillis 是 3 分钟，当前所有收到数据的最大时间戳是 08:15，watermark 就是 08:12，
            // 此时来了一条 08:13 的数据，大于 watermark 所以被缓存起来，然后到另一个流的缓存寻找匹配的数据，
            // 然后暂时不会删除，因为另一个流有可能还会有匹配的数据进来，
            // 假设 upperBound 是 1 分钟，后来又有 08:18 的数据进来，watermark 被更新为 08:15，
            // 超过了 08:13 + 00:01，所以 08:13 这条数据就会被删除，
            // 
            // 具体可以参考 IntervalJoinOperator
            
            result = watermarkStream_1
                    .keyBy(value -> value.getId())
                    .intervalJoin(watermarkStream_2.keyBy(event -> event.getId()))
                    .between(Time.seconds(-20), Time.seconds(20))
                    .process(new ProcessJoinFunction<KafkaValue, KafkaEvent, Result>() {
                        private static final long serialVersionUID = -4160822587066518079L;

                        @Override
                        public void processElement(KafkaValue left, KafkaEvent right, Context ctx, Collector<Result> out) {
                            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            
                            Result result = new Result();
                        
                            result.setValueId(left.getId());
                            result.setValueTime(sdf.format(left.getTime()));
                            result.setValue(left.getValue().doubleValue());
                        
                            result.setEventId(right.getId());
                            result.setEventTime(sdf.format(right.getTime()));
                            result.setEvent(right.getEvent());
                            
                            out.collect(result);
                        }
                    });
        } else {
            System.out.println("invalid join type");
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
            
            // 不知道如何从 join 流取出 Side Output
            
            // 这个结果是两个流在同一个窗口内的合并，没做窗口的聚合，可以理解为结果是一个延迟的、做了合并的流
            
            // 可能在后面再加上一个流的 Window 操作，再做窗口的聚合操作
            
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        } else if (sinkType == "console") {
            System.out.println("output to console");
            
            result.print();
            
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
    
    
    
    public <T> FlinkKafkaConsumer<T> getKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer) {
        // earliest 只有在找不到初始 offset 的时候才起作用, 如果设置了 group id, 就不起作用了,
        // 但如果不设 group id 的话那 Flink 会报错
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Flink-Test");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition(topic, 0), 0L);

        FlinkKafkaConsumer<T> kafkaConsumerCustom =
                new FlinkKafkaConsumer<>(topic, valueDeserializer, properties);
            
        // 强制从指定的 offset 开始读取
        kafkaConsumerCustom.setStartFromSpecificOffsets(specificStartOffsets);

        return kafkaConsumerCustom;        
    }
    
    public static class MyWatermarkStrategy<T extends KafkaBasic> implements WatermarkStrategy<T> {

        private static final long serialVersionUID = -1539493178965542934L;

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
        
        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<T>() {
                
                // 定义 Watermark，将所有收到的数据所携待的时间的最大值，减去 outOfOrdernessMillis 作为 Watermark
                // 假设窗口大小是 10 分钟，outOfOrdernessMillis 是 3 分钟，那么
                // （0，10）窗口需要在收到的数据时间大于等于 13 的时候才触发
                
                private long maxTimestamp = 0;
                private long outOfOrdernessMillis = 180000;

                @Override
                public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
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
                    
                    System.out.println("emitWatermark = " + (maxTimestamp - outOfOrdernessMillis - 1));
                    
                    // onPeriodicEmit 会被周期性调用
                    // 可以通过 ExecutionConfig#setAutoWatermarkInterval() 设置周期，设为 0 就不会调用
                    output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
                }
            };
        }
        
        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return new TimestampAssigner<T>() {

                // timestampAssigner 决定了如何获取 Event Time
                @Override
                public long extractTimestamp(T element, long recordTimestamp) {
                    return element.extractTime();
                }
            };
        }
    }
    
    abstract public static class KafkaBasic {
        abstract public long extractTime();
    }
    
    // inner class 必须是 static 不然会报无法序列化的错误
    public static class KafkaValue extends KafkaBasic {
        private String id;
        private Date time;
        private Integer value;
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
        public KafkaValue() {}
        
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
        public long extractTime() {
            return this.time.getTime();
        }
        
        @Override
        public String toString() {
            return "KafkaEvent(id=" + this.id + 
                   ", time=" + this.time + 
                   ", value=" + this.value + 
                   ")";
        }
    }
    
    // inner class 必须是 static 不然会报无法序列化的错误
    public static class KafkaEvent extends KafkaBasic {
        private String id;
        private Date time;
        private String event;
        
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
        
        public void setEvent(String event) {
            this.event = event;
        }
        
        public String getEvent() {
            return this.event;
        }
        
        @Override
        public long extractTime() {
            return this.time.getTime();
        }
        
        @Override
        public String toString() {
            return "KafkaEvent(id=" + this.id + 
                   ", time=" + this.time + 
                   ", event=" + this.event + 
                   ")";
        }
    }
    
    public static class KafkaSchema<T> implements DeserializationSchema<T>, SerializationSchema<T> {

        private static final long serialVersionUID = 5929973422365645403L;
        private ObjectMapper objectMapper;

        protected Class<T> entityClass;
        
        public KafkaSchema() {}
        
        public KafkaSchema(Class<T> entityClass) {
            this.entityClass = entityClass;
        }
        
        /*
        @SuppressWarnings("unchecked")
        public Class<T> getTClass()
        {
            // 这种方法获取 T 的类型，可能会报错：java.lang.Class cannot be cast to java.lang.reflect.ParameterizedType
            Class<T> tClass = (Class<T>)((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            return tClass;
        }
        
        @Override
        public TypeInformation<T> getProducedType() {
            return TypeInformation.of(getTClass());
        }
        */
        
        @Override
        public TypeInformation<T> getProducedType() {
            return TypeInformation.of(this.entityClass);
        }

        @Override
        public byte[] serialize(T element) {
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
        public T deserialize(byte[] message) throws IOException {
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
            
            return objectMapper.readValue(new String(message), this.entityClass);
        }

        @Override
        public boolean isEndOfStream(T nextElement) {
            return false;
        }
    }
    
    // inner class 必须是 static 不然会报无法序列化的错误
    public static class Result {
        private String valueId;
        private String eventId;
        private String valueTime;
        private String eventTime;
        private Double value;
        private String event;
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
        public Result() {}
        
        public void setValueId(String valueId) {
            this.valueId = valueId;
        }
        
        public String getValueId() {
            return this.valueId;
        }
        
        public void setEventId(String eventId) {
            this.eventId = eventId;
        }
        
        public String getEventId() {
            return this.eventId;
        }
        
        public void setValueTime(String valueTime) {
            this.valueTime = valueTime;
        }
        
        public String getValueTime() {
            return this.valueTime;
        }
        
        public void setEventTime(String eventTime) {
            this.eventTime = eventTime;
        }
        
        public String getEventTime() {
            return this.eventTime;
        }        
        
        public void setEvent(String event) {
            this.event = event;
        }
        
        public String getEvent() {
            return this.event;
        }
        
        public void setValue(Double value) {
            this.value = value;
        }
        
        public Double getValue() {
            return this.value;
        }
        
        @Override
        public String toString() {
            return "Result(valueId=" + this.valueId + 
                   ", valueTime=" + this.valueTime + 
                   ", value=" + this.value + 
                   ", eventId=" + this.eventId + 
                   ", eventTime=" + this.eventTime + 
                   ", event=" + this.event + 
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
        String sinkType;
        String joinType;
        
        Options options = new Options();
        options.addOption("s", "sink", true, "sink type: kafka or console");
        options.addOption("j", "join", true, "join type: join or joinAggregate or intervalJoin");
        
        CommandLineParser parser = new DefaultParser( );
        CommandLine commandLine = parser.parse( options, args );
        
        sinkType = commandLine.getOptionValue("s");
        joinType = commandLine.getOptionValue("j");
       
        /**
         * 启动 Kafka Stream 程序
         */
        KafkaStreamingJoinStreaming kafkaStreaming = new KafkaStreamingJoinStreaming();
        kafkaStreaming.execute(sinkType, joinType);
    }
}
