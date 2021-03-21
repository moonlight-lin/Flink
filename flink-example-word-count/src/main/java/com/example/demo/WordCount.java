package com.example.demo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;


public class WordCount {
    
    public void execute(String inputFile, String outputFile) {
        
        if (inputFile == null) {            
            inputFile = "src\\main\\java\\com\\example\\demo\\WordCount.java";
            String pwd = System.getProperty("user.dir");
            System.out.println("inputFile is null");
            System.out.println("current directory is " + pwd + ", change inputFile to " + inputFile);
        } else {
            System.out.println("inputFile is " + inputFile);
        } 
        
        if (outputFile == null) {
            System.out.println("outputFile is null, will print result to console");
        } else {
            System.out.println("outputFile is " + outputFile);
        }
        
        /**
         * 
         * 创建 ExecutionEnvironment 用于批处理 (对应的有 StreamExecutionEnvironment 用于流处理)
         * 
         * getExecutionEnvironment 底层有两个函数可以调用，会自行判断该调用哪个创建执行环境
         * 
         *   createRemoteEnvironment 用于创建集群执行环境，比如
         *   
         *     bin/flink run \                ## standalone, YARN (Session Mode) 等模式
         *               example.jar \
         *               --input README.txt
         *     
         *     bin/flink run \
         *               -p 3 \                            ## 并发度，默认 1
         *               -c com.example.demo.WordCount \   ## 如果 jar 包的 MANIFEST 没指定启动类话，需要自行指定
         *               example.jar \
         *               --input README.txt
         *   
         *     bin/flink run \
         *               -m yarn-cluster \             ## YARN (Per-Job Cluster Mode) 模式
         *               -ynm "Word Counter Test" \    ## Yarn job 的名字
         *               example.jar \
         *               --input README.txt
         *     
         *     bin/flink run \
         *               -t yarn-session \             ## YARN (Session Mode) 模式, 1.10 版本引入，效果和第一种一样
         *               example.jar \
         *               --input README.txt
         *     
         *     bin/flink run \
         *               -t yarn-per-job \             ## YARN (Per-Job Cluster Mode) 模式, 1.10 版本引入，效果和 -m yarn-cluster 一样
         *               example.jar \
         *               --input README.txt
         *                
         *     bin/flink run-application \
         *               -t yarn-application \         ## YARN (Application Mode) 模式, 1.10 版本引入
         *               example.jar \                 ##   Per-Job: 每个 Job 有专用集群，但 Client 还要做很多 submit 的前期工作
         *               --input README.txt            ##   Application: 每个 Job 有专用集群，但把原来 Client 做的很多工作放到 JobManager 做
         *                                             ##   官方更推荐 Application Mode
         *                                             ##   https://ci.apache.org/projects/flink/flink-docs-release-1.12/deployment/
         *                
         *     bin/flink run \
         *               -t kubernetes-session \       ## Kubernetes Session 模式
         *               example.jar \
         *               --input README.txt
         *               
         *     bin/flink run-application \
         *               -t kubernetes-application \   ## Kubernetes Application Mode 模式
         *               example.jar \
         *               --input README.txt
         *                                             
         *   createLocalEnvironment  用于创建本机执行环境，比如
         *              
         *     bin/flink run \
         *               -t local \           ## 好像不一定创建 local environment, 不同用法还不一样, 具体不清楚
         *               example.jar \
         *               --input README.txt
         *               
         *     java 命令调用：java -cp "xxx\flink-example-word-count\target\classes;xxx\.m2\xxx" com.example.demo.WordCount
         *     
         *     Eclipse 等 IDE 执行（也是调用 java 命令）
         *     
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        System.out.println("create ExecutionEnvironment " + env.getClass());
        
        // 读取文件的每一行作为一个 String 到 DataSet
        DataSet<String> text = env.readTextFile(inputFile);
        Preconditions.checkNotNull(text, "Input DataSet should not be null.");
        
        DataSet<Tuple2<String, Integer>> counts =
            text.flatMap(new Splitter())  // 将每行 String 拆成多个词，并赋予初始计数值 (word,1)
                .groupBy(0)               // group by the Tuple2 field "0" (word)
                .sum(1);                  // sum up Tuple2 field "1" (count)
        
        if (outputFile == null) {
            // 打印到 console
            try {
                // DataSet 的 print 函数里已经执行了 env.execute() 函数
                counts.print();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 写入文件
            counts.writeAsText(outputFile, WriteMode.OVERWRITE);
            try {
                // 需要执行 execute 函数
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * FlatMap 函数，输入 String，输出 Collector，并且 Collector 的元素是 Tuple2<String, Integer>
     */
    public final class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final long serialVersionUID = 6340943567159722481L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
    
    public static void main(String[] args) throws ParseException {
        /**
         * 解析输入参数
         */
        String inputFile;
        String outputFile;
        
        Options options = new Options();
        options.addOption("i", "input", true, "input file");
        options.addOption("o", "output", true, "output file");
        
        CommandLineParser parser = new DefaultParser( );
        CommandLine commandLine = parser.parse( options, args );
        
        inputFile = commandLine.getOptionValue("i");
        outputFile = commandLine.getOptionValue("o");
       
        /**
         * 启动 Word Count 程序
         */
        WordCount wordCount = new WordCount();
        wordCount.execute(inputFile, outputFile);
    }
}
