package com.example.demo;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
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
rule message
{"time": "2020-01-01T08:00:00Z", "operation": "add", "name": "rule-cancel-an-order", "pattern": "order,cancel"}
{"time": "2020-01-01T08:00:00Z", "operation": "add", "name": "rule-login-then-logout", "pattern": "login,logout"}

user message
{"id": "user-001", "time": "2020-01-01T08:00:01Z", "action": "login"}
{"id": "user-002", "time": "2020-01-01T08:00:02Z", "action": "login"}
{"id": "user-003", "time": "2020-01-01T08:00:03Z", "action": "login"}
{"id": "user-004", "time": "2020-01-01T08:00:04Z", "action": "view"}
{"id": "user-001", "time": "2020-01-01T08:01:01Z", "action": "view"}
{"id": "user-002", "time": "2020-01-01T08:01:02Z", "action": "view"}
{"id": "user-003", "time": "2020-01-01T08:01:03Z", "action": "logout"}
{"id": "user-004", "time": "2020-01-01T08:01:04Z", "action": "view"}
{"id": "user-001", "time": "2020-01-01T08:02:01Z", "action": "order"}
{"id": "user-002", "time": "2020-01-01T08:02:02Z", "action": "order"}
{"id": "user-003", "time": "2020-01-01T08:02:03Z", "action": "login"}
{"id": "user-004", "time": "2020-01-01T08:02:04Z", "action": "view"}
{"id": "user-001", "time": "2020-01-01T08:03:01Z", "action": "pay"}
{"id": "user-002", "time": "2020-01-01T08:03:02Z", "action": "cancel"}
{"id": "user-003", "time": "2020-01-01T08:03:03Z", "action": "logout"}
{"id": "user-004", "time": "2020-01-01T08:03:04Z", "action": "view"}


rule message
{"time": "2020-01-01T09:00:00Z", "operation": "delete", "name": "rule-login-then-logout"}
{"time": "2020-01-01T09:00:00Z", "operation": "add", "name": "rule-many-view", "pattern": "view,view,view,view"}

user message
{"id": "user-001", "time": "2020-01-01T09:00:01Z", "action": "login"}
{"id": "user-002", "time": "2020-01-01T09:00:02Z", "action": "login"}
{"id": "user-003", "time": "2020-01-01T09:00:03Z", "action": "login"}
{"id": "user-004", "time": "2020-01-01T09:00:04Z", "action": "view"}
{"id": "user-001", "time": "2020-01-01T09:01:01Z", "action": "view"}
{"id": "user-002", "time": "2020-01-01T09:01:02Z", "action": "view"}
{"id": "user-003", "time": "2020-01-01T09:01:03Z", "action": "logout"}
{"id": "user-004", "time": "2020-01-01T09:01:04Z", "action": "view"}
{"id": "user-001", "time": "2020-01-01T09:02:01Z", "action": "order"}
{"id": "user-002", "time": "2020-01-01T09:02:02Z", "action": "order"}
{"id": "user-003", "time": "2020-01-01T09:02:03Z", "action": "login"}
{"id": "user-004", "time": "2020-01-01T09:02:04Z", "action": "view"}
{"id": "user-001", "time": "2020-01-01T09:03:01Z", "action": "pay"}
{"id": "user-002", "time": "2020-01-01T09:03:02Z", "action": "cancel"}
{"id": "user-003", "time": "2020-01-01T09:03:03Z", "action": "logout"}
{"id": "user-004", "time": "2020-01-01T09:03:04Z", "action": "view"}
 */


/**
 * 
 * Broadcast 适用于需要关联高吞吐量的流数据和低吞吐量的静态数据的场景
 * 
 * 比如一个电商程序，需要按某种规则分析用户的行为，用户行为是实时流数据，而规则是静态数据很少需要改变
 * 
 * 如果希望规则改变的时候不需要重新配置，重新启动，那么 Broadcast 就是一个合适的选项
 *
 */
public class KafkaStreamingBroadcast {
    
    public void execute(String sinkType) {
        
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
        
        // ***************
        // 获取 Stream 环境
        // ***************
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        System.out.println("create ExecutionEnvironment " + env.getClass());
        
        env.enableCheckpointing(10000);   // 打开 checkpoint，默认是关闭的
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 设置一致性，默认就是 EXACTLY_ONCE
                        
        
        // ****************
        // 获取用户流和规则流
        // ****************
        DataStream<UserMessage> userMessageStream = env.addSource(
                getKafkaConsumer("flink-test-user", new KafkaSchema<UserMessage>(UserMessage.class))
                );
        
        DataStream<RuleMessage> ruleMessageStream = env.addSource(
                getKafkaConsumer("flink-test-rule", new KafkaSchema<RuleMessage>(RuleMessage.class))
                );
        
        // **************
        // 设置 watermark
        // **************
        KeyedStream<UserMessage, String> userMessageWindowedStream = userMessageStream
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy<UserMessage>())
                .filter((userMessage) -> StringUtils.isNotEmpty(userMessage.getAction()))
                .keyBy((userMessage) -> userMessage.getId());
        
        SingleOutputStreamOperator<RuleMessage> ruleMessageWatermarkStream = ruleMessageStream
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy<RuleMessage>())
                .filter((ruleMessage) -> StringUtils.isNotEmpty(ruleMessage.getOperation()));
        
        // ***********************
        // 设置用于存储广播数据的状态
        // ***********************
        MapStateDescriptor<String, RuleMessage> ruleStateDescriptor = new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<RuleMessage>() {}));
        
        // ***********************
        // 将状态广播到所有后续的操作
        // ***********************
        BroadcastStream<RuleMessage> ruleBroadcastStream = ruleMessageWatermarkStream
                .broadcast(ruleStateDescriptor);
        
        // *********************************************
        // 关联 non-broadcast stream 和 broadcast stream
        // *********************************************
        DataStream<Result> resultStream = 
                userMessageWindowedStream
                .connect(ruleBroadcastStream)
                .process(
                        // String : non-broadcast 的 Key 的类型
                        // UserMessage : non-broadcast 的数据类型
                        // RuleMessage : broadcast 的数据类型
                        // Result : 输出的数据类型
                        new KeyedBroadcastProcessFunction<String, UserMessage, RuleMessage, Result>() {
                            
                            private static final long serialVersionUID = -2314721464202933803L;

                            // 为了获取前面设置的广播状态
                            private final MapStateDescriptor<String, RuleMessage> ruleStateDescriptor = 
                                    new MapStateDescriptor<>(
                                            "RulesBroadcastState",
                                            BasicTypeInfo.STRING_TYPE_INFO,
                                            TypeInformation.of(new TypeHint<RuleMessage>() {}));
                            
                            // 用于保存匹配的中间结果
                            private final MapStateDescriptor<String, List<UserMessage>> matchStateDesc =
                                    new MapStateDescriptor<>(
                                            "matchItems",
                                            BasicTypeInfo.STRING_TYPE_INFO,
                                            new ListTypeInfo<>(UserMessage.class));
                            
                            @Override
                            public void processBroadcastElement(
                                    RuleMessage ruleMessage,
                                    KeyedBroadcastProcessFunction<String, UserMessage, RuleMessage, Result>.Context ctx,
                                    Collector<Result> out) throws Exception {
                                
                                // 消息会广播到所有 operator，如果并发度是 4，那么这个消息会打印 4 次
                                System.out.println("receive broadcast data : " + ruleMessage.toString());
                                
                                // 处理新的广播数据，通常都是频率比较低的数据
                                // 在这个例子中，用于添加或删除 rule
                                
                                if (ruleMessage.getOperation().equals("add")) {
                                    ctx.getBroadcastState(ruleStateDescriptor).put(ruleMessage.getName(), ruleMessage);
                                } else {
                                    ctx.getBroadcastState(ruleStateDescriptor).remove(ruleMessage.getName());
                                }
                            }

                            @Override
                            public void processElement(
                                    UserMessage userMessage,
                                    KeyedBroadcastProcessFunction<String, UserMessage, RuleMessage, Result>.ReadOnlyContext ctx,
                                    Collector<Result> out) throws Exception {
                                
                                // 处理业务数据
                                System.out.println("receive service data : " + userMessage.toString());
                                
                                // 获取部分匹配的状态
                                final MapState<String, List<UserMessage>> matchState = getRuntimeContext().getMapState(matchStateDesc);
                                
                                // 获取用户的操作
                                final String action = userMessage.getAction();
                            
                                // 便利所有配置的 rule
                                for (Map.Entry<String, RuleMessage> entry :
                                        ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {                                    
                                    
                                    final String ruleName = entry.getKey();
                                    final RuleMessage rule = entry.getValue();
                            
                                    // 获取当前 rule 的部分匹配结果
                                    List<UserMessage> partialMatchMessages = matchState.get(ruleName);
                                    
                                    // 没匹配过，先初始化
                                    if (partialMatchMessages == null) {
                                        partialMatchMessages = new ArrayList<>();
                                    }
                            
                                    // 获取 rule 的 pattern
                                    String[] patterns = rule.getPattern().split(",");
                                    
                                    // 在部分匹配的基础上，再看是否继续满足匹配条件
                                    int nextCheckIndex = partialMatchMessages.size();
                                    
                                    if (action.equals(patterns[nextCheckIndex])) {
                                        // 匹配
                                        
                                        if (nextCheckIndex == patterns.length - 1) {
                                            // 完全匹配，输出结果
                                            
                                            Result result = new Result();
                                            result.setUserId(userMessage.getId());
                                            result.setRuleName(ruleName);
                                            
                                            List<String> actions = new ArrayList<>();
                                            partialMatchMessages.add(userMessage);
                                            for (UserMessage msg : partialMatchMessages) {
                                                actions.add("[" + msg.getTime() + "](" + msg.getAction() + ")");
                                            }
                                            result.setActions(actions);
                                            
                                            System.out.println("match : " + result.toString());
                                            
                                            // 输出
                                            out.collect(result);
                                            
                                            // 清理中间结果
                                            partialMatchMessages.clear();
                                            
                                        } else {
                                            // 部分匹配，添加到中间结果
                                            partialMatchMessages.add(userMessage);
                                        }
                                    } else {
                                        // 不匹配，清理中间结果
                                        partialMatchMessages.clear();
                                    }
                                    
                                    // 更新状态
                                    //
                                    // state 包括 key state 和 operator state
                                    // 
                                    // 这里是 key state，这个 matchState 就是和 key (在这里就是 userId) 绑定的，所以不需要有针对 key 的操作
                                    matchState.put(ruleName, partialMatchMessages);
                                }
                            }
                        }
                );
        
        // *******
        // 输出结果
        // *******
        if (sinkType == "kafka") {
            System.out.println("output to Kafka");
            
            // 正常输出
            Properties outputProperties = new Properties();
            outputProperties.setProperty("bootstrap.servers", "localhost:9092");
        
            resultStream.addSink(new FlinkKafkaProducer<>(
                    "flink-output-topic",
                    new KafkaSchema<Result>(Result.class),
                    outputProperties));
            
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        } else if (sinkType == "console") {
            System.out.println("output to console");
            
            resultStream.print();
            
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
    public static class UserMessage extends KafkaBasic {
        private String id;
        private String action;
        private Timestamp time;   // Table 支持 long 或 Timestamp，不支持 Date
                                  // 而 ObjectMapper 支持日期转为 Timestamp 或 Date，不支持 long
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
        public UserMessage() {}
        
        public void setId(String id) {
            this.id = id;
        }
        
        public String getId() {
            return this.id;
        }
        
        public void setTime(Timestamp time) {
            this.time = time;
        }
        
        public Timestamp getTime() {
            return this.time;
        }
        
        public void setAction(String action) {
            this.action = action;
        }
        
        public String getAction() {
            return this.action;
        }
        
        @Override
        public long extractTime() {
            return this.time.getTime();
        }
        
        @Override
        public String toString() {
            return "UserMessage(id=" + this.id + 
                   ", time=" + this.time + 
                   ", action=" + this.action + 
                   ")";
        }
    }
    
    // inner class 必须是 static 不然会报无法序列化的错误
    public static class RuleMessage extends KafkaBasic {
        private String operation;
        private String name;
        private String pattern;
        private Timestamp time;   // Table 支持 long 或 Timestamp，不支持 Date
                                  // 而 ObjectMapper 支持日期转为 Timestamp 或 Date，不支持 long
            
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
        public RuleMessage() {}
        
        public void setOperation(String operation) {
            this.operation = operation;
        }
        
        public String getOperation() {
            return this.operation;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public String getName() {
            return this.name;
        }
        
        public void setPattern(String pattern) {
            this.pattern = pattern;
        }
        
        public String getPattern() {
            return this.pattern;
        }
        
        public void setTime(Timestamp time) {
            this.time = time;
        }
        
        public Timestamp getTime() {
            return this.time;
        }
        
        @Override
        public long extractTime() {
            return this.time.getTime();
        }
        
        @Override
        public String toString() {
            return "RuleMessage(operation=" + this.operation + 
                   ", name=" + this.name + 
                   ", pattern=" + this.pattern + 
                   ", time=" + this.time + 
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
                                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")));
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
        private String userId;
        private String ruleName;
        private List<String> actions;
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
        public Result() {}
        
        public void setUserId(String userId) {
            this.userId = userId;
        }
        
        public String getUserId() {
            return this.userId;
        }
        
        public void setRuleName(String ruleName) {
            this.ruleName = ruleName;
        }
        
        public String getRuleName() {
            return this.ruleName;
        }
        
        public void setActions(List<String> actions) {
            this.actions = actions;
        }
        
        public List<String> getActions() {
            return this.actions;
        }
        
        @Override
        public String toString() {
            return "Result(userId=" + this.userId + 
                   ", ruleName=" + this.ruleName + 
                   ", actions=" + this.actions + "" +
                   ")";
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
        
        Options options = new Options();
        options.addOption("s", "sink", true, "sink type: kafka or console");
        
        CommandLineParser parser = new DefaultParser( );
        CommandLine commandLine = parser.parse( options, args );
        
        sinkType = commandLine.getOptionValue("s");
       
        /**
         * 启动 Kafka Stream 程序
         */
        KafkaStreamingBroadcast kafkaStreaming = new KafkaStreamingBroadcast();
        kafkaStreaming.execute(sinkType);
    }
}
