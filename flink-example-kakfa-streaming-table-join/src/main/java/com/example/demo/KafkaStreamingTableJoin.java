package com.example.demo;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.and;


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
{"id": "id-001", "time": "2020-01-01T08:00:01Z", "value": 100}
{"id": "id-002", "time": "2020-01-01T08:00:02Z", "value": 200}
{"id": "id-003", "time": "2020-01-01T08:00:03Z", "value": 300}
{"id": "id-001", "time": "2020-01-01T08:17:01Z", "value": 117}
{"id": "id-002", "time": "2020-01-01T08:17:02Z", "value": 217}
{"id": "id-003", "time": "2020-01-01T08:17:03Z", "value": 317}
{"id": "id-001", "time": "2020-01-01T08:02:01Z", "value": 102}
{"id": "id-002", "time": "2020-01-01T08:02:02Z", "value": 202}
{"id": "id-003", "time": "2020-01-01T08:02:03Z", "value": 302}
{"id": "id-001", "time": "2020-01-01T08:03:01Z", "value": 103}
{"id": "id-002", "time": "2020-01-01T08:03:02Z", "value": 203}
{"id": "id-003", "time": "2020-01-01T08:03:03Z", "value": 303}
{"id": "id-001", "time": "2020-01-01T08:20:01Z", "value": 120}
{"id": "id-002", "time": "2020-01-01T08:20:02Z", "value": 220}
{"id": "id-003", "time": "2020-01-01T08:20:03Z", "value": 320}
{"id": "id-001", "time": "2020-01-01T08:05:01Z", "value": 105}
{"id": "id-002", "time": "2020-01-01T08:05:02Z", "value": 205}
{"id": "id-003", "time": "2020-01-01T08:05:03Z", "value": 305}
{"id": "id-001", "time": "2020-01-01T08:30:01Z", "value": 130}
{"id": "id-002", "time": "2020-01-01T08:30:02Z", "value": 230}
{"id": "id-003", "time": "2020-01-01T08:30:03Z", "value": 330}
{"id": "id-001", "time": "2020-01-01T08:47:01Z", "value": 147}
{"id": "id-002", "time": "2020-01-01T08:47:02Z", "value": 247}
{"id": "id-003", "time": "2020-01-01T08:47:03Z", "value": 347}

{"id": "id-001", "time": "2020-01-01T08:00:11Z", "event": "e-100"}
{"id": "id-002", "time": "2020-01-01T08:00:12Z", "event": "e-200"}
{"id": "id-003", "time": "2020-01-01T08:00:33Z", "event": "e-300"}
{"id": "id-001", "time": "2020-01-01T08:17:11Z", "event": "e-117"}
{"id": "id-002", "time": "2020-01-01T08:17:12Z", "event": "e-217"}
{"id": "id-003", "time": "2020-01-01T08:17:33Z", "event": "e-317"}
{"id": "id-001", "time": "2020-01-01T08:02:11Z", "event": "e-102"}
{"id": "id-002", "time": "2020-01-01T08:02:12Z", "event": "e-202"}
{"id": "id-003", "time": "2020-01-01T08:02:33Z", "event": "e-302"}
{"id": "id-001", "time": "2020-01-01T08:03:11Z", "event": "e-103"}
{"id": "id-002", "time": "2020-01-01T08:03:12Z", "event": "e-203"}
{"id": "id-003", "time": "2020-01-01T08:03:33Z", "event": "e-303"}
{"id": "id-001", "time": "2020-01-01T08:20:11Z", "event": "e-120"}
{"id": "id-002", "time": "2020-01-01T08:20:12Z", "event": "e-220"}
{"id": "id-003", "time": "2020-01-01T08:20:33Z", "event": "e-320"}
{"id": "id-001", "time": "2020-01-01T08:05:11Z", "event": "e-105"}
{"id": "id-002", "time": "2020-01-01T08:05:12Z", "event": "e-205"}
{"id": "id-003", "time": "2020-01-01T08:05:33Z", "event": "e-305"}
{"id": "id-001", "time": "2020-01-01T08:30:11Z", "event": "e-130"}
{"id": "id-002", "time": "2020-01-01T08:30:12Z", "event": "e-230"}
{"id": "id-003", "time": "2020-01-01T08:30:33Z", "event": "e-330"}
{"id": "id-001", "time": "2020-01-01T08:47:11Z", "event": "e-147"}
{"id": "id-002", "time": "2020-01-01T08:47:12Z", "event": "e-247"}
{"id": "id-003", "time": "2020-01-01T08:47:33Z", "event": "e-347"}
 */


public class KafkaStreamingTableJoin {
    
    public void execute(String sinkType, String formatType) {
        
        // *******
        // ????????????
        // *******
        if (sinkType == null) {
            sinkType = "console";
            System.out.println("sinkType is null, set to " + sinkType);
        } else if (sinkType != "kafka" && sinkType != "console") {
            System.out.println("invalid sink type: " + sinkType);
            return;
        } else {
            System.out.println("sinkType is : " + sinkType);
        }
        
        if (formatType == null) {
            formatType = "SQL-DDL";
            System.out.println("joinType is null, set to " + formatType);
        } else if (formatType != "tableApi" && formatType != "SQL-DDL") {
            System.out.println("invalid join type: " + formatType);
            return;
        } else {
            System.out.println("joinType is : " + formatType);
        }
        
        // ***************
        // ?????? Stream ??????
        // ***************
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        System.out.println("create ExecutionEnvironment " + env.getClass());
        
        env.enableCheckpointing(10000);   // ?????? checkpoint?????????????????????
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // ?????????????????????????????? EXACTLY_ONCE
        
        // *********************
        // ?????? Stream Table ??????
        // **********************
        
        // ?????? Blink
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        System.out.println("create TableEnvironment " + tableEnv.getClass());

        
        // *********************
        // ?????? Batch Table ??????
        // *********************
        
        boolean testBatch = false;
        
        if (testBatch) {
            
            // Blink ??? Batch Table ????????? Stream ?????????????????????????????? Flink ??? Batch Table ??????????????????????????????????????????????????????
            
            // Stream Table ??? Batch Table ?????? Join????????????: Only tables from the same TableEnvironment can be joined.
            
            EnvironmentSettings batchSettings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inBatchMode()
                    .build();
            
            TableEnvironment batchTableEnv = TableEnvironment.create(batchSettings);
            
            Table batchTable = batchTableEnv.fromValues(
                    DataTypes.ROW(
                            DataTypes.FIELD("id", DataTypes.STRING()),
                            DataTypes.FIELD("name", DataTypes.STRING())
                            ),
                    Row.of("id-001", "name-001"),
                    Row.of("id-002", "name-002")
            );
            
            batchTable.printSchema();
            
            batchTableEnv.createTemporaryView("profile_1", batchTable);
            
            batchTableEnv.sqlQuery("select * from profile_1").execute().print();
            
            batchTableEnv.executeSql(
                    "create table profile_2 (" + 
                       "id VARCHAR, " +
                       "age INT" +
                    ") WITH ( " +
                       "'connector.type' = 'jdbc', " +
                       "'connector.url'='jdbc:postgresql://localhost:5433/postgres', " +
                       "'connector.table' = 'profile', " +
                       "'connector.username' = 'postgres', " +
                       "'connector.password' = '123456'" +
                    ")"
            );
            
            batchTableEnv.sqlQuery("select * from profile_2").execute().print();
            
            batchTableEnv.sqlQuery("select * from profile_1 join profile_2 on profile_1.id = profile_2.id").execute().print();
            
            return;
        }
                        
        
        // **********************************
        // ????????? Table API ??? SQL ?????????????????? 
        // **********************************
        
        DataStream<Result> resultStream;
        
        if (formatType == "tableApi") {
            // **************
            // Table API ??????
            // **************
            
            // *****************
            // ?????? Kafka Source
            // *****************
                
            DataStream<KafkaValue> kafkaValueStream = env.addSource(
                    getKafkaConsumer("flink-test-1", new KafkaSchema<KafkaValue>(KafkaValue.class))
                    );
            
            DataStream<KafkaEvent> kafkaEventStream = env.addSource(
                    getKafkaConsumer("flink-test-2", new KafkaSchema<KafkaEvent>(KafkaEvent.class))
                    );
    
            // **************
            // ?????? watermark
            // **************
            
            SingleOutputStreamOperator<KafkaValue> kafkaValueWatermarkStream = kafkaValueStream             
                    .filter((kafkaValue) -> kafkaValue.getValue() > 0)
                    .assignTimestampsAndWatermarks(new MyWatermarkStrategy<KafkaValue>());
            
            SingleOutputStreamOperator<KafkaEvent> kafkaEventWatermarkStream = kafkaEventStream             
                    .filter((kafkaEvent) -> kafkaEvent.getEvent() != "")
                    .assignTimestampsAndWatermarks(new MyWatermarkStrategy<KafkaEvent>());
    
            // ********************
            // ??? stream ?????? table
            // ********************
            
            Table valueTable = tableEnv.fromDataStream(
                    kafkaValueWatermarkStream,
                    $("id"),                // String
                    $("time").rowtime(),    // Timestamp
                    $("value")              // Integer
                    );
            
            Table eventTable = tableEnv.fromDataStream(
                    kafkaEventWatermarkStream,
                    $("id"),                // String
                    $("time").rowtime(),    // Timestamp
                    $("event")              // String
                    );

            // ****************************
            // ?????? table API ?????? Join ??????
            // ****************************
            Table resultTable;
            
            boolean useWindow = false;
            
            if (!useWindow) {
                
                // *******
                // ????????????
                // *******
                
                valueTable = valueTable.renameColumns(
                        $("id").as("valueId"),
                        $("time").as("valueTime")
                        );
                
                eventTable = eventTable.renameColumns(
                        $("id").as("eventId"),
                        $("time").as("eventTime")
                        );
                
                // ?????????????????? window ?????? watermark ???????????????????????????
                // ????????????????????????????????????????????? watermark????????????????????? watermark ??????????????????
                // ???????????? 08:17:01 ?????????????????????????????? watermark ??? 08:14:01???????????? 08:02:01 ??????????????????
                // 
                // ????????? stream ??? interval join
                // 
                resultTable = 
                        valueTable.join(eventTable)
                                  .where(
                                      and(
                                          $("valueId").isEqual($("eventId")),
                                          $("valueTime").isGreaterOrEqual($("eventTime").minus(lit(20).second())),
                                          $("valueTime").isLess($("eventTime").plus(lit(20).second()))
                                      )
                                  )                              
                                  .select(
                                      $("valueId"),
                                      $("valueTime"),
                                      $("value"),
                                      $("eventId"),
                                      $("eventTime"),
                                      $("event")
                                  );
            } else {
            
                // ????????????????????????????????? window ??????????????????
                // ????????? rowtime ????????????????
                
                Table windowValueTable = valueTable.window(
                            Slide.over(lit(15).minutes())
                                 .every(lit(10).minutes())
                                 .on($("time"))
                                 .as("slide_window")
                        ).groupBy(
                            $("slide_window"), 
                            $("id")
                        ).select(
                            $("id").as("valueId"),
                            $("slide_window").start().as("valueTime"), 
                            $("value").cast(DataTypes.DOUBLE()).avg().as("value")
                        );
                
                Table windowEventTable = eventTable.window(
                            Slide.over(lit(15).minutes())
                                 .every(lit(10).minutes())
                                 .on($("time"))
                                 .as("slide_window")
                        ).groupBy(
                            $("slide_window"), 
                            $("id")
                        ).select(
                            $("id").as("eventId"),
                            $("slide_window").start().as("eventTime"), 
                            $("event").max().as("event")
                        );
                
                
                resultTable = 
                        windowValueTable.join(windowEventTable)
                                  .where(
                                      $("valueId").isEqual($("eventId"))
                                  )
                                  .select(
                                      $("valueId"),
                                      $("valueTime"),
                                      $("value"),
                                      $("eventId"),
                                      $("eventTime"),
                                      $("event")
                                  );
            }
           
            
            // ********************
            // ??? table ?????? stream
            // ********************
            resultStream = tableEnv.toAppendStream(resultTable, Result.class);
            
            // ??????????????????
            resultStream = resultStream.map(
                    (result) -> {
                        
                        Timestamp valueTimestamp = result.getValueTime();
                        Timestamp eventTimestamp =  result.getEventTime();
                        
                        long valueTime = valueTimestamp.getTime();
                        long eventTime = eventTimestamp.getTime();
                        
                        System.out.println("valueTime : " + valueTimestamp + ", " + valueTime);
                        System.out.println("eventTime : " + eventTimestamp + ", " + eventTime);
                        
                        long newValueTime = valueTime + 8 * 60 * 60 * 1000;
                        long newEventTime = eventTime + 8 * 60 * 60 * 1000;
                        
                        Timestamp newValueTimestampt = new Timestamp(newValueTime);
                        Timestamp newEventTimestamp = new Timestamp(newEventTime); 
                        
                        System.out.println("after convertion, valueTime : " + newValueTimestampt + ", " + newValueTime);
                        System.out.println("after convertion, eventTime : " + newEventTimestamp + ", " + newEventTime);
                        
                        result.setValueTime(newValueTimestampt);
                        result.setEventTime(newEventTimestamp);
                        
                        return result;
                    }
            );
            
        } else if (formatType == "SQL-DDL") {
            // ******************            
            // ?????? DDL ?????? Table
            // ******************
            
            tableEnv.executeSql(
                    "create table Kafka_Value_Source (" +
                      // "`partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  " +  // ??? metadata ?????? partition ??? offset
                      // "`offset` BIGINT METADATA VIRTUAL, " +
                      "id STRING, " +
                      "`value` BIGINT, " +
                      "`time` TIMESTAMP(3), " +
                      "WATERMARK FOR `time` AS `time` - INTERVAL '3' MINUTE " +      // ?????? watermark ?????????????????????????????? rowtime ??????
                    ") with (" +
                      "'connector.type' = 'kafka', " +
                      "'connector.version' = 'universal', " +
                      "'connector.properties.bootstrap.servers' = 'localhost:9092', " +
                      "'connector.topic' = 'flink-test-1', " +
                      "'connector.properties.group.id' = 'testGroup', " +
                      "'connector.startup-mode' = 'specific-offsets', " +
                      "'connector.specific-offsets' = 'partition:0,offset:0', " +
                      "'update-mode' = 'append', " +
                      "'format.type' = 'json'" +
                    ")"
                    );
            
                    // ??????????????? connector ?????????????????????
                    // ????????????????????? version ???????????????????????? 'topic' = '', ???????????? scan.startup.specific-offsets 
                    // ??????????????????????????????????????????
            
                    // DDL ????????? watermark ??????????????????????????????
                    // (??????????????????????????? interval ??????????????????)
            
            tableEnv.executeSql(
                    "create table Kafka_Event_Source (" +
                      // "`partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  " +  // ??? metadata ?????? partition ??? offset
                      // "`offset` BIGINT METADATA VIRTUAL, " +
                      "id STRING, " +
                      "`event` STRING, " +
                      "`time` TIMESTAMP(3), " +
                      "WATERMARK FOR `time` AS `time` - INTERVAL '3' MINUTE " +      // ?????? watermark ?????????????????????????????? rowtime ??????
                    ") with (" +
                      "'connector.type' = 'kafka', " +
                      "'connector.version' = 'universal', " +
                      "'connector.properties.bootstrap.servers' = 'localhost:9092', " +
                      "'connector.topic' = 'flink-test-2', " +
                      "'connector.properties.group.id' = 'testGroup', " +
                      "'connector.startup-mode' = 'specific-offsets', " +
                      "'connector.specific-offsets' = 'partition:0,offset:0', " +
                      "'update-mode' = 'append', " +
                      "'format.type' = 'json'" +
                    ")"
                    );
            
            if (sinkType == "kafka") {
                
                // ????????????????????? table ?????????
                // ??? Kafka ????????????????????? Kafka ???????????? insert ?????????????????????????????????????????????????????? Topic
                
                // create ??????????????????????????? select ???????????????
                
                // ??????????????????????????? SQL DDL ????????????????????????????????????????????????????????????????????????????????????????????????????????????
                
                tableEnv.executeSql(
                        "create table Kafka_Sink (" +
                          "valueId STRING, " +
                          "eventId STRING, " +
                          "valueTime TIMESTAMP(3), " +
                          "eventTime TIMESTAMP(3), " +
                          "`value` Double, " +
                          "`event` STRING" +
                        ") with (" +
                          "'connector.type' = 'kafka', " +
                          "'connector.version' = 'universal', " +
                          "'connector.properties.bootstrap.servers' = 'localhost:9092', " +
                          "'connector.topic' = 'flink-output-topic', " +
                          "'update-mode' = 'append', " +
                          "'format.type' = 'json'" +
                        ")"
                        );
                
                // ??? source kafka ????????????????????????????????????????????? sink kafka
                
                // ?????????????????? window ?????? watermark ???????????????????????????
                // ????????????????????????????????????????????? watermark????????????????????? watermark ??????????????????
                // ???????????? 08:17:01 ?????????????????????????????? watermark ??? 08:14:01???????????? 08:02:01 ??????????????????
                // 
                // ????????? stream ??? interval join
                // 
                
                tableEnv.executeSql(
                        "insert into Kafka_Sink " +
                        "select " + 
                          "Kafka_Value_Source.id as valueId, " +
                          "Kafka_Event_Source.id as eventId, " +
                          "Kafka_Value_Source.`time` as valueTime, " +
                          "Kafka_Event_Source.`time` as eventTime, " +
                          "Kafka_Value_Source.`value` as `value`, " +
                          "Kafka_Event_Source.`event` as `event` " +
                        "from " +
                          "Kafka_Value_Source " +
                        "join " +
                          "Kafka_Event_Source " +
                        "on " +
                          "Kafka_Value_Source.id = Kafka_Event_Source.id and " +
                          "Kafka_Value_Source.`time` > Kafka_Event_Source.`time` - INTERVAL '20' SECOND and " +
                          "Kafka_Value_Source.`time` < Kafka_Event_Source.`time` + INTERVAL '20' SECOND " +
                        "where " +
                          "`value` > 100 "
                        );
                
                try {
                    tableEnv.execute("test");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
                return;
            } else {
                // ????????? Kafka?????????????????? SQL
                Table resultTable = tableEnv.sqlQuery(
                        "select " + 
                          "Kafka_Value_Source.id as valueId, " +
                          "Kafka_Event_Source.id as eventId, " +
                          "Kafka_Value_Source.`time` as valueTime, " +
                          "Kafka_Event_Source.`time` as eventTime, " +
                          "Kafka_Value_Source.`value` as `value`, " +
                          "Kafka_Event_Source.`event` as `event` " +
                        "from " +
                          "Kafka_Value_Source " +
                        "join " +
                          "Kafka_Event_Source " +
                        "on " +
                          "Kafka_Value_Source.id = Kafka_Event_Source.id and " +
                          "Kafka_Value_Source.`time` > Kafka_Event_Source.`time` - INTERVAL '20' SECOND and " +
                          "Kafka_Value_Source.`time` < Kafka_Event_Source.`time` + INTERVAL '20' SECOND " +
                        "where " +
                          "`value` > 100 "
                        );
                
                // ????????? stream
                resultStream = tableEnv.toAppendStream(resultTable, Result.class);
            }
           
        } else {
            System.out.println("invalid formatType : " + formatType);
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
    public static class KafkaValue extends KafkaBasic {
        private String id;
        private Integer value;
        private Timestamp time;   // Table ?????? long ??? Timestamp???????????? Date
                                  // ??? ObjectMapper ?????????????????? Timestamp ??? Date???????????? long
        
        // ?????????????????????????????? setter ??? getter ????????????????????????????????????????????????
        public KafkaValue() {}
        
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
            return "KafkaValue(id=" + this.id + 
                   ", time=" + this.time + 
                   ", value=" + this.value + 
                   ")";
        }
    }
    
    // inner class ????????? static ????????????????????????????????????
    public static class KafkaEvent extends KafkaBasic {
        private String id;
        private String event;
        private Timestamp time;   // Table ?????? long ??? Timestamp???????????? Date
                                  // ??? ObjectMapper ?????????????????? Timestamp ??? Date???????????? long
        
        // ?????????????????????????????? setter ??? getter ????????????????????????????????????????????????
        public KafkaEvent() {}
        
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
        private String valueId;
        private String eventId;
        private Timestamp valueTime;
        private Timestamp eventTime;
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
        
        public void setValueTime(Timestamp valueTime) {
            this.valueTime = valueTime;
        }
        
        public Timestamp getValueTime() {
            return this.valueTime;
        }
        
        public void setEventTime(Timestamp eventTime) {
            this.eventTime = eventTime;
        }
        
        public Timestamp getEventTime() {
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
    
    public static void main(String[] args) throws ParseException {
        /**
         * ???????????? pom.xml ?????? log ????????????????????????????????????
         */
        
        /**
         * ??????????????????
         */
        String sinkType;
        String formatType;
        
        Options options = new Options();
        options.addOption("s", "sink", true, "sink type: kafka or console");
        options.addOption("f", "format", true, "format type: tableApi or SQL-Connect or SQL-DDL");
        
        CommandLineParser parser = new DefaultParser( );
        CommandLine commandLine = parser.parse( options, args );
        
        sinkType = commandLine.getOptionValue("s");
        formatType = commandLine.getOptionValue("f");
       
        /**
         * ?????? Kafka Stream ??????
         */
        KafkaStreamingTableJoin kafkaStreaming = new KafkaStreamingTableJoin();
        kafkaStreaming.execute(sinkType, formatType);
    }
}
