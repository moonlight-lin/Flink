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
        // ?????? Stream ??????
        // ***************
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        System.out.println("create ExecutionEnvironment " + env.getClass());
        
        env.enableCheckpointing(10000);   // ?????? checkpoint?????????????????????
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // ?????????????????????????????? EXACTLY_ONCE
        
        // *****************
        // ?????? Kafka Source
        // *****************
            
        DataStream<KafkaValue> kafkaStream_1 = env.addSource(
                getKafkaConsumer("flink-test-1", new KafkaSchema<KafkaValue>(KafkaValue.class))
                );
        
        DataStream<KafkaEvent> kafkaStream_2 = env.addSource(
                getKafkaConsumer("flink-test-2", new KafkaSchema<KafkaEvent>(KafkaEvent.class))
                );

        // **************
        // ?????? watermark
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
            // ?????? join ???????????????????????????????????????????????? join ???????????????
            // ???????????? stream ????????? (08:00 ~ 08:15) ??? 10 ?????????????????? join ???????????? 100 ???
            // watermark ????????????????????? window ??????????????????????????????????????? watermark ???????????????????????? join ??????
            
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
            // ?????? join ??????????????????????????????
            // ??????????????? stream ??????????????? a???????????????????????? stream ?????????????????? a ????????? between(lowerBound, upperBound) ??????????????????,
            // ???????????????????????? a ???????????? join ??????,
            // 
            // watermark ????????? MyWatermarkStrategy ?????????????????? watermark ??????????????????
            // ???????????????????????? watermark ?????????????????????????????????
            // ?????? outOfOrdernessMillis ??? 3 ?????????????????????????????????????????????????????? 08:15???
            // ????????????????????????????????????????????? 08:12 ??????????????????
            //
            // ?????????????????????????????????????????????????????????????????????????????????????????? watermark ??? upperBound ?????????
            // ??? watermark ?????????????????? + upperBound ?????????????????????????????????
            // ?????? outOfOrdernessMillis ??? 3 ?????????????????????????????????????????????????????? 08:15???watermark ?????? 08:12???
            // ?????????????????? 08:13 ?????????????????? watermark ??????????????????????????????????????????????????????????????????????????????
            // ???????????????????????????????????????????????????????????????????????????????????????
            // ?????? upperBound ??? 1 ????????????????????? 08:18 ??????????????????watermark ???????????? 08:15???
            // ????????? 08:13 + 00:01????????? 08:13 ??????????????????????????????
            // 
            // ?????????????????? IntervalJoinOperator
            
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
        // ????????????
        // *******
        if (sinkType == "kafka") {
            System.out.println("output to Kafka");
            
            // ????????????
            Properties outputProperties = new Properties();
            outputProperties.setProperty("bootstrap.servers", "localhost:9092");
        
            result.addSink(new FlinkKafkaProducer<>(
                    "flink-output-topic",
                    new ResultSchema(),
                    outputProperties));
            
            // ?????????????????? join ????????? Side Output
            
            // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
            
            // ???????????????????????????????????? Window ????????????????????????????????????
            
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
    public static class KafkaValue extends KafkaBasic {
        private String id;
        private Date time;
        private Integer value;
        
        // ?????????????????????????????? setter ??? getter ????????????????????????????????????????????????
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
    
    // inner class ????????? static ????????????????????????????????????
    public static class KafkaEvent extends KafkaBasic {
        private String id;
        private Date time;
        private String event;
        
        // ?????????????????????????????? setter ??? getter ????????????????????????????????????????????????
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
                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")));
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
        private String valueId;
        private String eventId;
        private String valueTime;
        private String eventTime;
        private Double value;
        private String event;
        
        // ?????????????????????????????? setter ??? getter ????????????????????????????????????????????????
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
         * ???????????? pom.xml ?????? log ????????????????????????????????????
         */
        
        /**
         * ??????????????????
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
         * ?????? Kafka Stream ??????
         */
        KafkaStreamingJoinStreaming kafkaStreaming = new KafkaStreamingJoinStreaming();
        kafkaStreaming.execute(sinkType, joinType);
    }
}
