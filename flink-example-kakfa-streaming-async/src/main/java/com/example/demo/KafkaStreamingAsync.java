package com.example.demo;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;
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


/*
 * 实际应用中，流数据有可能要实时地去关联维度数据，
 * 维度数据大多存在外部系统，涉及 IO 操作，并且有可能会变化，所以严格来讲，要每次都去取数据才行，
 * 由于有 IO 操作，如果采用同步方法，会有大量等待时间，严重损耗性能，
 * 最好是采用异步方法，这样执行操作后，不用等待返回，可以执行下一个操作，有效提高性能
 */
public class KafkaStreamingAsync {
    
    public void execute(String sinkType) {
        
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
        
        // earliest 只有在找不到初始 offset 的时候才起作用, 如果设置了 group id, 就不起作用了,
        // 但如果不设 group id 的话那 Flink 会报错
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Flink-Test");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        String topic = "flink-test";

        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition(topic, 0), 0L);

        FlinkKafkaConsumer<KafkaEvent> kafkaConsumerCustom =
                new FlinkKafkaConsumer<>(topic, new KafkaEventSchema(), properties);
            
        // 强制从指定的 offset 开始读取
        kafkaConsumerCustom.setStartFromSpecificOffsets(specificStartOffsets);
            
        DataStream<KafkaEvent> eventStream = env.addSource(kafkaConsumerCustom);

        // ************************
        // 设置 watermark 和 window
        // ************************
        
        final OutputTag<KafkaEvent> lateOutputTag = new OutputTag<KafkaEvent>("late-data"){
            private static final long serialVersionUID = 1L;
        };
        
        WindowedStream<KafkaEvent, String, TimeWindow> windowedStream = eventStream
                // 定义 watermark，取 time 字段做 EventTime，且 EndTime 小于最大 EventTime 超过 3 分钟的 window 会被触发，超 10 分钟没数据进入 idle
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy().withIdleness(Duration.ofMinutes(10)))
                .filter((event) -> event.getValue() > 0)                
                .keyBy((event) -> event.getId())                
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(10)))                
                .sideOutputLateData(lateOutputTag);
        
        // *****************
        //  处理 window 数据
        // *****************
        
        SingleOutputStreamOperator<Result> result = windowedStream.aggregate(
                new MyAggregateFunction(),          // 计算 window 内的平均值
                new MyProcessWindowFunction()       // 添加 window 信息
                );
        
        // ************
        //  异步扩展数据
        // ************
        
        DataStream<Result> enrichStream = 
                AsyncDataStream.orderedWait(   // orderedWait 保证先进先出，unorderedWait 不保证
                        result,                // 要处理的 stream
                        new MyAsyncFunction(), // 异步操作函数
                        1000,                  // 异步操作如果在 timeout 时间内没响应就认为是出错
                        TimeUnit.SECONDS,      // timeout 的单位
                        100                    // 最多可以有多少个异步请求同时在处理中，超过限制就不再执行，并可能会触发背压
                );
        
        // *******
        // 输出结果
        // *******
        if (sinkType == "kafka") {
            System.out.println("output to Kafka");
            
            // 正常输出
            Properties outputProperties = new Properties();
            outputProperties.setProperty("bootstrap.servers", "localhost:9092");
        
            enrichStream.addSink(new FlinkKafkaProducer<>(
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
            
            enrichStream.print();
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
    
    public static class MyWatermarkStrategy implements WatermarkStrategy<KafkaEvent> {

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
                    
                    System.out.println("emitWatermark = " + (maxTimestamp - outOfOrdernessMillis - 1));
                    
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
    }
    
    public static class MyAggregateFunction implements AggregateFunction<KafkaEvent, Tuple2<Long, Long>, Double> {
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
    }
    
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Double, Result, String, TimeWindow> {
        // 如果只有 AggregateFunction 那么 windowedStream.aggregate 的最终输出就是 AggregateFunction::getResult 的输出
        // 加上 ProcessWindowFunction 后，getResult 的值会传给 ProcessWindowFunction::process 进一步处理
        // 这里的目的是给 AggregateFunction::getResult 的输出加上 window 信息
        private static final long serialVersionUID = 1L;

        @Override
        public void process(
                String key,
                ProcessWindowFunction<Double, Result, String, TimeWindow>.Context context,
                Iterable<Double> element,
                Collector<Result> out) 
                        throws Exception {
                
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
    }
    
    public static class MyAsyncFunction extends RichAsyncFunction<Result, Result> {

        /*
         * 异步操作继承 RichAsyncFunction，需要定义输入类型和输出类型
         * 
         * 需要初始化或销毁操作可以重载 open 和 close
         * 
         * 需要对 timeout 做特殊处理的，可以重载 timeout 函数，timeout 函数的默认行为是调用
         *    resultFuture.completeExceptionally(
         *        new TimeoutException("Async function call has timed out."));
         * 
         * asyncInvoke 定义异步操作，具体可以参考 AsyncWaitOperator
         * 
         * AsyncWaitOperator 收到 element 的时候，会 new 一个 ResultFuture（flink 自己实现的类），
         * 然后把 element 和 ResultFuture 作为参数，调用 asyncInvoke
         * 
         * asyncInvoke 实现一个异步操作，
         * 并且要在这个异步操作完成的时候（比如在异步操作的回调函数里，或者将同步操作封装在线程里执行实现异步），
         * 将异步操作的结果，作为参数，调用 ResultFuture 的 complete 函数
         * 
         * ResultFuture 的 complete 函数，会调用另一个 thread 处理，把结果输出，触发 stream 的继续执行
         * 
         * 再简单梳理一下
         *    Stream 调用 asyncInvoke（element，ResultFuture）
         *    asyncInvoke 针对 element 做了一个异步操作
         *    Stream 继续其他工作
         *    异步操作完成后，通过 ResultFuture 输出结果
         *    下游的 Stream 收到数据并被触发
         */
        
        private static final long serialVersionUID = 4167034483105995482L;

        private transient ExecutorService executorService;
        
        @Override
        public void open(Configuration parameters) {
            System.out.println("open async");
            executorService = Executors.newFixedThreadPool(10);
        }
        
        @Override
        public void close() throws Exception {
            ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, executorService);
            System.out.println("close async");
        }
        
        @Override
        public void asyncInvoke(Result input, ResultFuture<Result> resultFuture) throws Exception {
            /*
             * 这里通过线程实现简单的异步操作
             * 
             * executorService = Executors.newFixedThreadPool(10) 获得线程池
             * 
             * executorService.submit 提交一个程序给线程执行，会返回一个 Future<T> 变量用于获取线程的状态和结果
             * 
             * executorService.execute 类似但不会返回 Future
             * 
             * 线程是异步执行的，不会影响当前线程，比如
             * 
             *     Future<String> a = executorService.submit(
             *             () -> {
             *                 Thread.sleep(5000);
             *                 return "Test";
             *             }
             *     );
             *
             *     // submit 后会立刻返回，不会等待线程完成，这里不断检查线程是否完成
             *     while (!a.isDone()) {
             *         Thread.sleep(100);
             *     }
             *
             *     // 获取线程的输出结果，如果调用 get 时线程还没完成，那么 get 操作会阻塞，一直等待直到线程执行完
             *     System.out.println(a.get());
             * 
             * Future 是 Java 1.5 引入的接口，代表一个正在执行的异步操作，用于获取这个异步操作的状态和结果
             * 
             * Future 需要不断检查判断是否完成，不是很方便
             * 
             * CompletableFuture 是 Java 1.8 引入的接口，对 Future 做了一些扩展，比如
             * 
             *     // supplyAsync 会调用线程执行函数
             *     CompletableFuture<String> a = CompletableFuture.supplyAsync(
             *             () -> {
             *                 try {
             *                     System.out.println(new Date() + " in thread");
             *                     Thread.sleep(5000);
             *                 } catch (InterruptedException e) {
             *                     e.printStackTrace();
             *                 }
             *                 
             *                 return "Test";
             *             }
             *     );
             *
             *     // 如果不需要 a 变量的话，thenAccept 可以直接跟在 supplyAsync 后面，那样更直观
             *     // 在异步操作完成后，就会自动调用 thenAccept 接收的函数
             *     a.thenAccept(
             *             (String result) -> {
             *                 System.out.println(new Date() + " in accept");
             *                 System.out.println(result + " in accept");
             *             }
             *     );
             *
             *     // 同样也可以通过不断检查线程是否完成来实现
             *     while (!a.isDone()) {
             *         System.out.println(new Date() + " in while");
             *         Thread.sleep(500);                
             *     }
             *
             *     System.out.println(a.get() + " in get");
             */
            
            executorService.submit(   // 提交程序到线程，然后就返回，这样就模拟了一个异步操作
                    () -> {
                        System.out.println("Before enrichment : " + input.toString());
                        
                        Result output = new Result();
                        output.setId(input.getId());
                        output.setValue(input.getValue());
                        output.setWindowStartTime(input.getWindowStartTime());
                        output.setWindowEndTime(input.getWindowEndTime());
                        
                        String id = input.getId();
                        
                        // 
                        try {
                            Thread.sleep(5000);     // 为了测试，增加运行时间，不影响住线程的继续运行
                            
                            String name = "name_" + Integer.parseInt(id.split("-")[1]);
                            
                            output.setName(name);
                        } catch (InterruptedException e) {
                            output.setName(id);
                        }
                        
                        System.out.println("After enrichment : " + output.toString());
                        
                        // 这步最重要
                        // 通过 resultFuture.complete 输出结果，触发下游的 Stream
                        resultFuture.complete(Collections.singletonList(output));
                    }
            );
            
            
            // 通过 CompletableFuture 执行，然后在 thenAccept 里面调用 resultFuture.complete 输出结果也可以
            
            
            /*
             * 如果目标操作本来就支持异步，并且有回调函数，就不需要专门使用线程模拟，比如
             * 
             *    import org.elasticsearch.client.RestHighLevelClient;
             *    import org.elasticsearch.action.search.SearchRequest;
             *    import org.elasticsearch.action.search.SearchResponse;
             *    import org.elasticsearch.action.ActionListener;
             *    
             *    restHighLevelClient.searchAsync(searchRequest, new ActionListener<SearchResponse>() {
             *        @Override
             *        public void onResponse(SearchResponse searchResponse) {
             *            output = ....;
             *            
             *            resultFuture.complete(Collections.singleton(output));
             *        }
             *    }
             */
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
        private String name;
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
        
        public void setName(String name) {
            this.name = name;
        }
        
        public String getName() {
            return this.name;
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
                   ", name=" + this.name + 
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
        String sinkType;
        
        Options options = new Options();
        options.addOption("s", "sink", true, "sink type: kafka or console");
        
        CommandLineParser parser = new DefaultParser( );
        CommandLine commandLine = parser.parse( options, args );
        
        sinkType = commandLine.getOptionValue("s");
       
        /**
         * 启动 Kafka Stream 程序
         */
        KafkaStreamingAsync kafkaStreaming = new KafkaStreamingAsync();
        kafkaStreaming.execute(sinkType);
    }
}
