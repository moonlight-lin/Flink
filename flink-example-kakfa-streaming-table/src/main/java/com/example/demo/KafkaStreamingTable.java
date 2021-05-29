package com.example.demo;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
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
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;


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
{"id": "id-001", "time": "2020-01-01T08:32:01Z", "value": 132}
{"id": "id-002", "time": "2020-01-01T08:32:02Z", "value": 232}
{"id": "id-003", "time": "2020-01-01T08:32:03Z", "value": 332}
{"id": "id-001", "time": "2020-01-01T08:33:01Z", "value": 133}
{"id": "id-002", "time": "2020-01-01T08:33:02Z", "value": 233}
{"id": "id-003", "time": "2020-01-01T08:33:03Z", "value": 333}
{"id": "id-001", "time": "2020-01-01T08:50:01Z", "value": 150}
{"id": "id-002", "time": "2020-01-01T08:50:02Z", "value": 250}
{"id": "id-003", "time": "2020-01-01T08:50:03Z", "value": 350}
{"id": "id-001", "time": "2020-01-01T08:35:01Z", "value": 135}
{"id": "id-002", "time": "2020-01-01T08:35:02Z", "value": 235}
{"id": "id-003", "time": "2020-01-01T08:35:03Z", "value": 335}
 */


public class KafkaStreamingTable {
    
    public void execute(String sinkType, String formatType) {
        
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
        
        if (formatType == null) {
            formatType = "SQL-DDL";
            System.out.println("joinType is null, set to " + formatType);
        } else if (formatType != "tableApi" && formatType != "SQL-Connect" && formatType != "SQL-DDL") {
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
        
        // **************
        // 获取 Table 环境
        // **************
        
        // 使用 Blink
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        System.out.println("create TableEnvironment " + tableEnv.getClass());
        
        // 尝试解决 rowtime() 导致的时区问题，但这两个设置都解决不了
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.setLocalTimeZone(ZoneId.of("UTC+8"));
        
        Configuration configuration = tableConfig.getConfiguration();
        configuration.setString("table.local-time-zone", "GMT+8:00");

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
                
            DataStream<KafkaValue> kafkaStream = env.addSource(
                    getKafkaConsumer("flink-test", new KafkaSchema<KafkaValue>(KafkaValue.class))
                    );
    
            // **************
            // 设置 watermark
            // **************
            
            SingleOutputStreamOperator<KafkaValue> watermarkStream = kafkaStream             
                    .filter((kafkaValue) -> kafkaValue.getValue() > 0)
                    .map((kafkaValue) -> {
                        // 可以看到此时的日期和对应的 long 类型时间戳还是对的
                        System.out.println("In map : " + kafkaValue.getTime() + ", " + kafkaValue.getTime().getTime());
                        return kafkaValue;
                    })
                    .assignTimestampsAndWatermarks(new MyWatermarkStrategy<KafkaValue>());
    
            // ********************
            // 将 stream 转为 table
            // ********************
            
            // 这里指定的 id、time、value 会到 KafkaValue 类寻找对应的变量名，及其类型，然后将其作为 table 的列使用，
            // 如果要使用 window 操作，那需要通过 rowtime() 函数指定对应列作为时间列，rowtime() 接收 long 或 timestamp 类型，
            //
            // rowtime() 函数的输出，把时间的时区搞错了，不管 time 是 timestamp 类似还是 long 类型都一样，不管有没有使用 window 也一样，
            // 通过在下一步直接 select 输出可以证明这一点，
            //
            //    Table resultTable = kafkaTable
            //            .select($("id"),
            //                    $("time").as("windowStart"), 
            //                    $("value").cast(DataTypes.DOUBLE()).as("avgValue"));
            // 
            // 这可能是 flink 的 bug，
            // http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/table-rowtime-timezome-problem-td40300.html
            
            Table kafkaTable = tableEnv.fromDataStream(
                    watermarkStream,
                    $("id"),                // String
                    $("time").rowtime(),    // Timestamp
                    $("value")              // Integer
                    );
            
            // *****************
            // 设置 slide window
            // *****************
            GroupWindowedTable windowedTable = kafkaTable.window(
                    Slide.over(lit(15).minutes())
                         .every(lit(10).minutes())
                         .on($("time"))
                         .as("slide_window")
                    );
            
            // **********************
            // 通过 table API 执行查询
            // **********************
            Table resultTable = windowedTable
                    .groupBy(
                            $("slide_window"), 
                            $("id")
                    )
                    .select(
                            $("id"),
                            $("id").count().cast(DataTypes.INT()).as("count"),
                            $("slide_window").start().as("windowStart"), 
                            $("slide_window").end().as("windowEnd"),                     // Timestamp
                            $("value").cast(DataTypes.DOUBLE()).avg().as("avgValue"));   // Double, 取 group by 的平均值
            
            // table API 提供了很多操作，具体可以查看
            //   https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/tableApi.html
            // 
            // (里面有个 Over Window Aggregation 可以将每条记录的前后多少条以前，作为一个窗口)
            // (和 SQL 的 over partition by 用法不大一样)
            
            // ********************
            // 将 table 转回 stream
            // ********************
            resultStream = tableEnv.toAppendStream(resultTable, Result.class);
            
            // 解决时区问题
            resultStream = resultStream.map(
                    (result) -> {
                        
                        Timestamp tsStart = result.getWindowStart();
                        Timestamp tsEnd =  result.getWindowEnd();
                        
                        long start = tsStart.getTime();
                        long end = tsEnd.getTime();
                        
                        System.out.println("start : " + tsStart + ", " + start);
                        System.out.println("end : " + tsEnd + ", " + end);
                        
                        long newStart = start + 8 * 60 * 60 * 1000;
                        long newEnd = end + 8 * 60 * 60 * 1000;
                        
                        Timestamp newTsStart = new Timestamp(newStart);
                        Timestamp newTsEnd = new Timestamp(newEnd); 
                        
                        System.out.println("after convertion, start : " + newTsStart + ", " + newStart);
                        System.out.println("after convertion, end : " + newTsEnd + ", " + newEnd);
                        
                        result.setWindowStart(newTsStart);
                        result.setWindowEnd(newTsEnd);
                        
                        return result;
                    }
            );
            
        } else if (formatType == "SQL-Connect") {
            // **************************            
            // 通过 Connect 函数创建 Table
            // **************************
            
            // **************************
            // 这种模式被 @deprecated 了，
            // 建议使用 DDL 模式创建 Table
            // **************************
            
            // 设置每个 partition 的起始 offset
            Map<Integer, Long> offsets = new HashMap<>();
            offsets.put(0, 0L);

            // connect 到数据源，这里以 Kafka 为例
            tableEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic("flink-test")
                        .startFromSpecificOffsets(offsets)
                        .property("bootstrap.servers", "localhost:9092")
                        .property("group.id", "Flink-Test")
            )
            .withFormat(
                new Json().failOnMissingField(false)    // 数据源为 Json 格式，
                                                        // 按后面定义的 schema 解析，如果 schema 定义的 field 不存在则设置为 null，
                                                        // 貌似解析时间只支持 yyyy-MM-dd'T'HH:mm:ss[.S]'Z' 格式
            )
            .withSchema(
                new Schema()
                        .field("id", DataTypes.STRING())   // id 既是 json 数据的 field，又是 table 的 column，如果用不同名字可以加上 .from 函数
                        .field("value", DataTypes.INT())
                        
                         // 为了使用 window 必须使用 rowtime 函数指定时间字段，并且 field 函数和 rowtime 函数必须先后写在一起，
                         // timestampsFromField 的名字必须是 Json 的名字，
                         // field 的名字可以随便写，但不能和 timestampsFromField 的名字一样，
                         // 和 tableApi 不一样，这里的 rowtime 函数不会有时区问题
                        .field("rowtime_temp", DataTypes.TIMESTAMP(3))  
                        .rowtime(new Rowtime()
                                     .timestampsFromField("time")
                                     .watermarksPeriodicBounded(3 * 60 * 1000))   // 使用内置的 watermark
            )
            .inAppendMode()
            .createTemporaryTable("Kafka");   // table 的名字
            
            
            // SQL 有很多保留字，强制使用需要通过 `` 符号转义
            
            // HOP 函数指定 slide 窗口，HOP_START 和 HOP_END 获取窗口的起止时间
            // 还有 TUMBLE 窗口和 SESSION 窗口
            
            // timestamp(3) 的 3 指的是精度，秒后面有多少位
            
            // 这里取窗口内 value 的平均值
            
            Table resultTable = tableEnv.sqlQuery(
                    "select " + 
                      "id, " +
                      "cast(count(id) as int) as `count`, " +
                      "cast(HOP_START(rowtime_temp, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE) as timestamp(3)) as windowStart, " +
                      "HOP_END(rowtime_temp, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE) as windowEnd, " +
                      "avg(cast(`value` as double)) as avgValue " +
                    "from " +
                      "Kafka " +
                    "where " +
                      "`value` > 0 " +
                    "group by " +
                      "id, " +
                      "HOP(rowtime_temp, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE)"
                    );
            
            // 将结果转为 stream
            resultStream = tableEnv.toAppendStream(resultTable, Result.class);
            
        } else if (formatType == "SQL-DDL") {
            // ******************            
            // 通过 DDL 创建 Table
            // ******************
            
            tableEnv.executeSql(
                    "create table Kafka_Source (" +
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
                      "'connector.topic' = 'flink-test', " +
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
            
            if (sinkType == "kafka") {
                
                // 输出也可以通过 table 实现，
                // 以 Kafka 为例，就是创建 Kafka 表，然后 insert 数据到这张表，就相当于发送数据到这个 Topic
                
                // create 的字段的顺序必须和 select 的顺序一致

                // 如果输入输出都通过 SQL DDL 实现，那实际上不需要定义接受数据和输出数据的类，少了很多代码，而且更灵活
                
                tableEnv.executeSql(
                        "create table Kafka_Sink (" +
                          "id STRING, " +
                          "`count` INT, " +
                          "windowStart TIMESTAMP(3), " +
                          "windowEnd TIMESTAMP(3), " +
                          "avgValue DOUBLE" +
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
                tableEnv.executeSql(
                        "insert into Kafka_Sink " +
                        "select " + 
                          "id, " +
                          "cast(count(id) as int) as `count`, " +
                          "cast(HOP_START(`time`, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE) as timestamp(3)) as windowStart, " +
                          "HOP_END(`time`, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE) as windowEnd, " +
                          "avg(cast(`value` as double)) as avgValue " +
                        "from " +
                          "Kafka_Source " +
                        "where " +
                          "`value` > 0 " +
                        "group by " +
                          "id, " +
                          "HOP(`time`, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE)"
                        );
                
                try {
                    // 执行 tableEnv 的 execute 或是 streamEnv 的 execute 都可以
                    tableEnv.execute("test");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
                return;
            } else {
                // 不写到 Kafka，那就只执行 SQL
                Table resultTable = tableEnv.sqlQuery(
                        "select " + 
                          "id, " +
                          "cast(count(id) as int) as `count`, " +
                          "cast(HOP_START(`time`, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE) as timestamp(3)) as windowStart, " +
                          "HOP_END(`time`, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE) as windowEnd, " +
                          "avg(cast(`value` as double)) as avgValue " +
                        "from " +
                          "Kafka_Source " +
                        "where " +
                          "`value` > 0 " +
                        "group by " +
                          "id, " +
                          "HOP(`time`, INTERVAL '10' MINUTE, INTERVAL '15' MINUTE)"
                        );
                
                // 再转回 stream
                resultStream = tableEnv.toAppendStream(resultTable, Result.class);
            }
           
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
            return "KafkaEvent(id=" + this.id + 
                   ", time=" + this.time + 
                   ", value=" + this.value + 
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
        private String id;
        private Timestamp windowStart;
        private Timestamp windowEnd;
        private Integer count;
        private Double avgValue;
        
        // 最好把默认构造函数和 setter 和 getter 都加上，防止出现无法序列化的错误
        public Result() {}
        
        public void setId(String id) {
            this.id = id;
        }
        
        public String getId() {
            return this.id;
        }
        
        public void setWindowStart(Timestamp windowStart) {
            this.windowStart = windowStart;
        }
        
        public Timestamp getWindowStart() {
            return this.windowStart;
        }
        
        public void setWindowEnd(Timestamp windowEnd) {
            this.windowEnd = windowEnd;
        }
        
        public Timestamp getWindowEnd() {
            return this.windowEnd;
        }
        
        public void setCount(Integer count) {
            this.count = count;
        }
        
        public Integer getCount() {
            return this.count;
        }
        
        public void setAvgValue(Double avgValue) {
            this.avgValue = avgValue;
        }
        
        public Double getAvgValue() {
            return this.avgValue;
        }
        
        @Override
        public String toString() {            
            return "Result(id=" + this.id + 
                   ", windowStart=" + this.windowStart + 
                   ", windowEnd=" + this.windowEnd +
                   ", count=" + this.count +
                   ", avgValue=" + this.avgValue +
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
        KafkaStreamingTable kafkaStreaming = new KafkaStreamingTable();
        kafkaStreaming.execute(sinkType, formatType);
    }
}
