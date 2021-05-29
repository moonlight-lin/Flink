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
        // 验证参数
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
        // 获取 Stream 环境
        // ***************
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        System.out.println("create ExecutionEnvironment " + env.getClass());
        
        env.enableCheckpointing(10000);   // 打开 checkpoint，默认是关闭的
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 设置一致性，默认就是 EXACTLY_ONCE
        
        // *********************
        // 获取 Stream Table 环境
        // **********************
        
        // 使用 Blink
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        System.out.println("create TableEnvironment " + tableEnv.getClass());

        
        // *********************
        // 创建 Batch Table 环境
        // *********************
        
        boolean testBatch = false;
        
        if (testBatch) {
            
            // Blink 的 Batch Table 认为是 Stream 的特例，用起来似乎和 Flink 的 Batch Table 差异比较大，初始化以及用的类都不一样
            
            // Stream Table 和 Batch Table 无法 Join，会报错: Only tables from the same TableEnvironment can be joined.
            
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
        // 可以有 Table API 和 SQL 两种处理方式 
        // **********************************
        
        DataStream<Result> resultStream;
        
        if (formatType == "tableApi") {
            // **************
            // Table API 方式
            // **************
            
            // *****************
            // 设置 Kafka Source
            // *****************
                
            DataStream<KafkaValue> kafkaValueStream = env.addSource(
                    getKafkaConsumer("flink-test-1", new KafkaSchema<KafkaValue>(KafkaValue.class))
                    );
            
            DataStream<KafkaEvent> kafkaEventStream = env.addSource(
                    getKafkaConsumer("flink-test-2", new KafkaSchema<KafkaEvent>(KafkaEvent.class))
                    );
    
            // **************
            // 设置 watermark
            // **************
            
            SingleOutputStreamOperator<KafkaValue> kafkaValueWatermarkStream = kafkaValueStream             
                    .filter((kafkaValue) -> kafkaValue.getValue() > 0)
                    .assignTimestampsAndWatermarks(new MyWatermarkStrategy<KafkaValue>());
            
            SingleOutputStreamOperator<KafkaEvent> kafkaEventWatermarkStream = kafkaEventStream             
                    .filter((kafkaEvent) -> kafkaEvent.getEvent() != "")
                    .assignTimestampsAndWatermarks(new MyWatermarkStrategy<KafkaEvent>());
    
            // ********************
            // 将 stream 转为 table
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
            // 通过 table API 执行 Join 查询
            // ****************************
            Table resultTable;
            
            boolean useWindow = false;
            
            if (!useWindow) {
                
                // *******
                // 重命名列
                // *******
                
                valueTable = valueTable.renameColumns(
                        $("id").as("valueId"),
                        $("time").as("valueTime")
                        );
                
                eventTable = eventTable.renameColumns(
                        $("id").as("eventId"),
                        $("time").as("eventTime")
                        );
                
                // 由于没有使用 window 所以 watermark 是针对单个数据的，
                // 收到数据后会立刻处理，然后更新 watermark，新收到的小于 watermark 的数据丢弃，
                // 比如收到 08:17:01 后立刻处理，然后更新 watermark 为 08:14:01，再然后 08:02:01 到来会被丢弃
                // 
                // 类似于 stream 的 interval join
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
            
                // 如果在这里先做了重命名 window 操作会出错，
                // 是不是 rowtime 不能重命名?
                
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
            // 将 table 转回 stream
            // ********************
            resultStream = tableEnv.toAppendStream(resultTable, Result.class);
            
            // 解决时区问题
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
            // 通过 DDL 创建 Table
            // ******************
            
            tableEnv.executeSql(
                    "create table Kafka_Value_Source (" +
                      // "`partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  " +  // 从 metadata 获取 partition 和 offset
                      // "`offset` BIGINT METADATA VIRTUAL, " +
                      "id STRING, " +
                      "`value` BIGINT, " +
                      "`time` TIMESTAMP(3), " +
                      "WATERMARK FOR `time` AS `time` - INTERVAL '3' MINUTE " +      // 设置 watermark 的字段和策略，不需要 rowtime 函数
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
            
                    // 不同版本的 connector 的格式不一样，
                    // 比如有的不需要 version 字段，并且直接用 'topic' = '', 直接使用 scan.startup.specific-offsets 
                    // 出错信息有提示哪些字段不支持
            
                    // DDL 创建的 watermark 似乎是收到数据就触发
                    // (其他方式应该是有个 interval 会有一定间隔)
            
            tableEnv.executeSql(
                    "create table Kafka_Event_Source (" +
                      // "`partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  " +  // 从 metadata 获取 partition 和 offset
                      // "`offset` BIGINT METADATA VIRTUAL, " +
                      "id STRING, " +
                      "`event` STRING, " +
                      "`time` TIMESTAMP(3), " +
                      "WATERMARK FOR `time` AS `time` - INTERVAL '3' MINUTE " +      // 设置 watermark 的字段和策略，不需要 rowtime 函数
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
                
                // 输出也可以通过 table 实现，
                // 以 Kafka 为例，就是创建 Kafka 表，然后 insert 数据到这张表，就相当于发送数据到这个 Topic
                
                // create 的字段的顺序必须和 select 的顺序一致
                
                // 如果输入输出都通过 SQL DDL 实现，那实际上不需要定义接受数据和输出数据的类，少了很多代码，而且更灵活
                
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
                
                // 从 source kafka 读数据，窗口聚合，处理，再写回 sink kafka
                
                // 由于没有使用 window 所以 watermark 是针对单个数据的，
                // 收到数据后会立刻处理，然后更新 watermark，新收到的小于 watermark 的数据丢弃，
                // 比如收到 08:17:01 后立刻处理，然后更新 watermark 为 08:14:01，再然后 08:02:01 到来会被丢弃
                // 
                // 类似于 stream 的 interval join
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
                // 不写到 Kafka，那就只执行 SQL
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
                
                // 再转回 stream
                resultStream = tableEnv.toAppendStream(resultTable, Result.class);
            }
           
        } else {
            System.out.println("invalid formatType : " + formatType);
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
    public static class KafkaValue extends KafkaBasic {
        private String id;
        private Integer value;
        private Timestamp time;   // Table 支持 long 或 Timestamp，不支持 Date
                                  // 而 ObjectMapper 支持日期转为 Timestamp 或 Date，不支持 long
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
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
    
    // inner class 必须是 static 不然会报无法序列化的错误
    public static class KafkaEvent extends KafkaBasic {
        private String id;
        private String event;
        private Timestamp time;   // Table 支持 long 或 Timestamp，不支持 Date
                                  // 而 ObjectMapper 支持日期转为 Timestamp 或 Date，不支持 long
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
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
        private String valueId;
        private String eventId;
        private Timestamp valueTime;
        private Timestamp eventTime;
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
         * 注意要在 pom.xml 添加 log 依赖才能看到日志方便调试
         */
        
        /**
         * 解析输入参数
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
         * 启动 Kafka Stream 程序
         */
        KafkaStreamingTableJoin kafkaStreaming = new KafkaStreamingTableJoin();
        kafkaStreaming.execute(sinkType, formatType);
    }
}
