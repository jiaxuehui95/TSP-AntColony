/**
 * @author jiaxuehui
 * @version 1.0
 * @Description TODO
 * @Date 12/12/2018 10:38 PM
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

/**
 * @author jiaxuehui
 * @version 1.0
 * @Description 求最高气温和最低气温
 * @Date 12/12/2018 10:05 PM
 */



public class temperature {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        if (args == null || args.length < 2) {
            System.err.println("Parameter Errors! Usages:<inputpath> <outputpath>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = new Configuration();
        String jobName = temperature.class.getSimpleName();
        Job job = Job.getInstance(conf, jobName);
        //设置job运行的jar
        job.setJarByClass(temperature.class);
        //设置整个程序的输入
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);//就是设置如何将输入文件解析成一行一行内容的解析类
        //设置mapper
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        //设置整个程序的输出
        // outputpath.getFileSystem(conf).delete(outputpath, true);//如果当前输出目录存在，删除之，以避免.FileAlreadyExistsException
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置reducer
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //指定程序有几个reducer去运行
        job.setNumReduceTasks(1);
        //提交程序
        job.waitForCompletion(true);
    }

    public static class WeatherMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String line = v1.toString();
            Double max = null;
            Double min = null;
            try {
                // 获取一行中的气温MAX值
                max = Double.parseDouble(line.substring(103, 108));
                // 获取一行中的气温MIN值
                min = Double.parseDouble(line.substring(111, 116));
            } catch (NumberFormatException e) {
                // 如果出现异常，则当前的这一个map task不执行，直接返回
                return;
            }
            // 写到context中
            context.write(new Text("MAX"), new DoubleWritable(max));
            context.write(new Text("MIN"), new DoubleWritable(min));
        }
    }

    public static class WeatherReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {
            // 先预定义最大和最小气温值
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            // 得到迭代列表中的气温最大值和最小值
            if ("MAX".equals(k2.toString())) {
                for (DoubleWritable v2 : v2s) {
                    double tmp = v2.get();
                    if (tmp > max) {
                        max = tmp;
                    }
                }
            } else {
                for (DoubleWritable v2 : v2s) {
                    double tmp = v2.get();
                    if (tmp < min) {
                        min = tmp;
                    }
                }
            }
            // 将结果写入到context中
            context.write(k2, "MAX".equals(k2.toString()) ? new DoubleWritable(max) : new DoubleWritable(min));
        }
    }
}