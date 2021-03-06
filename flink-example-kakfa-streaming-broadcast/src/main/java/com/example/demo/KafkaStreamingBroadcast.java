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
Kafka ??????
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
Kafka ??????
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
 * Broadcast ????????????????????????????????????????????????????????????????????????????????????
 * 
 * ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
 * 
 * ?????????????????????????????????????????????????????????????????????????????? Broadcast ???????????????????????????
 *
 */
public class KafkaStreamingBroadcast {
    
    public void execute(String sinkType) {
        
        // *******
        // ????????????
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
        // ?????? Stream ??????
        // ***************
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        System.out.println("create ExecutionEnvironment " + env.getClass());
        
        env.enableCheckpointing(10000);   // ?????? checkpoint?????????????????????
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // ?????????????????????????????? EXACTLY_ONCE
                        
        
        // ****************
        // ???????????????????????????
        // ****************
        DataStream<UserMessage> userMessageStream = env.addSource(
                getKafkaConsumer("flink-test-user", new KafkaSchema<UserMessage>(UserMessage.class))
                );
        
        DataStream<RuleMessage> ruleMessageStream = env.addSource(
                getKafkaConsumer("flink-test-rule", new KafkaSchema<RuleMessage>(RuleMessage.class))
                );
        
        // **************
        // ?????? watermark
        // **************
        KeyedStream<UserMessage, String> userMessageWindowedStream = userMessageStream
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy<UserMessage>())
                .filter((userMessage) -> StringUtils.isNotEmpty(userMessage.getAction()))
                .keyBy((userMessage) -> userMessage.getId());
        
        SingleOutputStreamOperator<RuleMessage> ruleMessageWatermarkStream = ruleMessageStream
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy<RuleMessage>())
                .filter((ruleMessage) -> StringUtils.isNotEmpty(ruleMessage.getOperation()));
        
        // ***********************
        // ???????????????????????????????????????
        // ***********************
        MapStateDescriptor<String, RuleMessage> ruleStateDescriptor = new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<RuleMessage>() {}));
        
        // ***********************
        // ???????????????????????????????????????
        // ***********************
        BroadcastStream<RuleMessage> ruleBroadcastStream = ruleMessageWatermarkStream
                .broadcast(ruleStateDescriptor);
        
        // *********************************************
        // ?????? non-broadcast stream ??? broadcast stream
        // *********************************************
        DataStream<Result> resultStream = 
                userMessageWindowedStream
                .connect(ruleBroadcastStream)
                .process(
                        // String : non-broadcast ??? Key ?????????
                        // UserMessage : non-broadcast ???????????????
                        // RuleMessage : broadcast ???????????????
                        // Result : ?????????????????????
                        new KeyedBroadcastProcessFunction<String, UserMessage, RuleMessage, Result>() {
                            
                            private static final long serialVersionUID = -2314721464202933803L;

                            // ???????????????????????????????????????
                            private final MapStateDescriptor<String, RuleMessage> ruleStateDescriptor = 
                                    new MapStateDescriptor<>(
                                            "RulesBroadcastState",
                                            BasicTypeInfo.STRING_TYPE_INFO,
                                            TypeInformation.of(new TypeHint<RuleMessage>() {}));
                            
                            // ?????????????????????????????????
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
                                
                                // ???????????????????????? operator????????????????????? 4?????????????????????????????? 4 ???
                                System.out.println("receive broadcast data : " + ruleMessage.toString());
                                
                                // ???????????????????????????????????????????????????????????????
                                // ?????????????????????????????????????????? rule
                                
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
                                
                                // ??????????????????
                                System.out.println("receive service data : " + userMessage.toString());
                                
                                // ???????????????????????????
                                final MapState<String, List<UserMessage>> matchState = getRuntimeContext().getMapState(matchStateDesc);
                                
                                // ?????????????????????
                                final String action = userMessage.getAction();
                            
                                // ????????????????????? rule
                                for (Map.Entry<String, RuleMessage> entry :
                                        ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {                                    
                                    
                                    final String ruleName = entry.getKey();
                                    final RuleMessage rule = entry.getValue();
                            
                                    // ???????????? rule ?????????????????????
                                    List<UserMessage> partialMatchMessages = matchState.get(ruleName);
                                    
                                    // ???????????????????????????
                                    if (partialMatchMessages == null) {
                                        partialMatchMessages = new ArrayList<>();
                                    }
                            
                                    // ?????? rule ??? pattern
                                    String[] patterns = rule.getPattern().split(",");
                                    
                                    // ??????????????????????????????????????????????????????????????????
                                    int nextCheckIndex = partialMatchMessages.size();
                                    
                                    if (action.equals(patterns[nextCheckIndex])) {
                                        // ??????
                                        
                                        if (nextCheckIndex == patterns.length - 1) {
                                            // ???????????????????????????
                                            
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
                                            
                                            // ??????
                                            out.collect(result);
                                            
                                            // ??????????????????
                                            partialMatchMessages.clear();
                                            
                                        } else {
                                            // ????????????????????????????????????
                                            partialMatchMessages.add(userMessage);
                                        }
                                    } else {
                                        // ??????????????????????????????
                                        partialMatchMessages.clear();
                                    }
                                    
                                    // ????????????
                                    //
                                    // state ?????? key state ??? operator state
                                    // 
                                    // ????????? key state????????? matchState ????????? key (??????????????? userId) ???????????????????????????????????? key ?????????
                                    matchState.put(ruleName, partialMatchMessages);
                                }
                            }
                        }
                );
        
        // *******
        // ????????????
        // *******
        if (sinkType == "kafka") {
            System.out.println("output to Kafka");
            
            // ????????????
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
        // earliest ???????????????????????? offset ?????????????????????, ??????????????? group id, ??????????????????,
        // ??????????????? group id ????????? Flink ?????????
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Flink-Test");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition(topic, 0), 0L);

        FlinkKafkaConsumer<T> kafkaConsumerCustom =
                new FlinkKafkaConsumer<>(topic, valueDeserializer, properties);
            
        // ?????????????????? offset ????????????
        kafkaConsumerCustom.setStartFromSpecificOffsets(specificStartOffsets);

        return kafkaConsumerCustom;        
    }
    
    public static class MyWatermarkStrategy<T extends KafkaBasic> implements WatermarkStrategy<T> {

        private static final long serialVersionUID = -1539493178965542934L;

        // watermark ??????????????????????????? TimestampsAndWatermarksOperator
        // 
        // ??????????????????????????? watermark strategy ??? timestampAssigner ??? watermarkGenerator
        // ???????????? getExecutionConfig().getAutoWatermarkInterval(); ????????????????????? timer
        // 
        // ????????????????????????????????????????????? timestampAssigner ??????????????? event time
        // collect ?????????
        // ????????? watermarkGenerator.onEvent ??????
        //
        // ??????????????? timer????????????????????? watermarkGenerator.onPeriodicEmit(wmOutput);
        // ??????????????? timer
        
        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<T>() {
                
                // ?????? Watermark?????????????????????????????????????????????????????????????????? outOfOrdernessMillis ?????? Watermark
                // ????????????????????? 10 ?????????outOfOrdernessMillis ??? 3 ???????????????
                // ???0???10??????????????????????????????????????????????????? 13 ??????????????????
                
                private long maxTimestamp = 0;
                private long outOfOrdernessMillis = 180000;

                @Override
                public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
                    System.out.println("eventTimestamp = " + eventTimestamp + ", maxTimestamp = " + maxTimestamp);
                    
                    // onEvent ???????????????????????????????????????eventTimestamp ????????????????????????????????? timestampAssigner ????????????
                    // ????????? emit watermark ???????????????
                    maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    if (maxTimestamp == 0) {
                        return;
                    }
                    
                    System.out.println("emitWatermark = " + (maxTimestamp - outOfOrdernessMillis - 1));
                    
                    // onPeriodicEmit ?????????????????????
                    // ???????????? ExecutionConfig#setAutoWatermarkInterval() ????????????????????? 0 ???????????????
                    output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
                }
            };
        }
        
        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return new TimestampAssigner<T>() {

                // timestampAssigner ????????????????????? Event Time
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
    
    // inner class ????????? static ????????????????????????????????????
    public static class UserMessage extends KafkaBasic {
        private String id;
        private String action;
        private Timestamp time;   // Table ?????? long ??? Timestamp???????????? Date
                                  // ??? ObjectMapper ?????????????????? Timestamp ??? Date???????????? long
        
        // ?????????????????????????????? setter ??? getter ????????????????????????????????????????????????
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
    
    // inner class ????????? static ????????????????????????????????????
    public static class RuleMessage extends KafkaBasic {
        private String operation;
        private String name;
        private String pattern;
        private Timestamp time;   // Table ?????? long ??? Timestamp???????????? Date
                                  // ??? ObjectMapper ?????????????????? Timestamp ??? Date???????????? long
            
        
        // ?????????????????????????????? setter ??? getter ????????????????????????????????????????????????
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
            // ?????????????????? T ??????????????????????????????java.lang.Class cannot be cast to java.lang.reflect.ParameterizedType
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
                
                // ?????? mapper ??????????????????????????????
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
                
                // ?????? mapper ??????????????????????????????
                objectMapper.setConfig(
                        objectMapper.getDeserializationConfig().with(
                                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")));
            }
            
            System.out.println("message = " + new String(message));
            
            // ???????????????????????????????????????????????????????????????????????????????????? watermark ???????????? 200 ms ???????????????
            // ????????? watermark ?????????????????????????????????????????????????????????????????? side output ??? case
            // ???????????????????????????????????? 500 ms ??????????????????????????????????????? watermark
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
    
    // inner class ????????? static ????????????????????????????????????
    public static class Result {
        private String userId;
        private String ruleName;
        private List<String> actions;
        
        // ?????????????????????????????? setter ??? getter ????????????????????????????????????????????????
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
         * ???????????? pom.xml ?????? log ????????????????????????????????????
         */
        
        /**
         * ??????????????????
         */
        String sinkType;
        
        Options options = new Options();
        options.addOption("s", "sink", true, "sink type: kafka or console");
        
        CommandLineParser parser = new DefaultParser( );
        CommandLine commandLine = parser.parse( options, args );
        
        sinkType = commandLine.getOptionValue("s");
       
        /**
         * ?????? Kafka Stream ??????
         */
        KafkaStreamingBroadcast kafkaStreaming = new KafkaStreamingBroadcast();
        kafkaStreaming.execute(sinkType);
    }
}
