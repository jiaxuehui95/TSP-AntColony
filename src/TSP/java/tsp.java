/**
 * @author jiaxuehui
 * @version 1.0
 * @Description TODO
 * @Date 04/01/2019 4:04 PM
 */
import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class tsp {
    private static final Logger logger = Logger.getLogger(tsp.class.getName());


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();//Log4j是一个功能强大的日志组件,提供方便的日志记录，默认配置

        int index = 0;

        // 写距离矩阵
        int[] x;
        int[] y;
        String strbuff;
        BufferedReader data = new BufferedReader(new InputStreamReader(
                new FileInputStream("src/TSP/citys.txt")));
        int cityNum = Integer.parseInt(data.readLine());
        List<IntWritable> list = new LinkedList<IntWritable>();
        List<FloatWritable> phe = new LinkedList<FloatWritable>();
        IntWritable [][] distanceWritable = new IntWritable[cityNum][cityNum];


        x = new int[cityNum];
        y = new int[cityNum];
        for (int i = 0; i < cityNum; i++) {
            // 读取一行数据，数据格式1 6734 1453
            strbuff = data.readLine();
            // 字符分割
            String[] strcol = strbuff.split(" ");
            x[i] = Integer.valueOf(strcol[1]);// x坐标
            y[i] = Integer.valueOf(strcol[2]);// y坐标
        }
        data.close();
        // 计算距离矩阵
        // 针对具体问题，距离计算方法也不一样，此处用的是att48作为案例，它有48个城市，距离计算方法为伪欧氏距离，最优值为10628
        for (int i = 0; i < cityNum - 1; i++) {
            distanceWritable[i][i] = new IntWritable(0);
            for (int j = i + 1; j < cityNum; j++) {

                double rij = Math
                        .sqrt(((x[i] - x[j]) * (x[i] - x[j]) + (y[i] - y[j])
                                * (y[i] - y[j])) / 10.0);
                // 四舍五入，取整
                int tij = (int) Math.round(rij);
                if (tij < rij) {
                    distanceWritable[i][j]=new IntWritable(tij + 1);
                    distanceWritable[j][i]=distanceWritable[i][j];

                } else {
                    distanceWritable[i][j]=new IntWritable(tij);
                    distanceWritable[j][i]=distanceWritable[i][j];
                }
            }

        }


        distanceWritable[cityNum-1][cityNum-1]=new IntWritable(0);
        BufferedWriter pheW=new BufferedWriter(new OutputStreamWriter(new FileOutputStream("src/TSP/Pheromone.txt")));
        for (int i=0;i<cityNum;i++) {
            for (int j = 0; j < cityNum; j++) {
                list.add(distanceWritable[i][j]);
                pheW.write("0.1 ");
            }
            pheW.write("\n");
        }
        pheW.close();


        BufferedReader ACOargs = new BufferedReader(new InputStreamReader(new FileInputStream("src/TSP/ACOargs.txt")));
        int N = Integer.parseInt(ACOargs.readLine());
        float a = Float.parseFloat(ACOargs.readLine());
        float b = Float.parseFloat(ACOargs.readLine());
        float r = Float.parseFloat(ACOargs.readLine());
        ACOargs.close();


        Path inputPath = new Path("src/TSP/antGroups.txt");


        Configuration conf = new Configuration();
        while (index<N){  //N 次迭代
            if(index!=0){
                inputPath = new Path("src/TSP/output"+(index-1)+"/part-r-00000");
            }
            Path outputPath = new Path("src/TSP/output"+index);

            conf.setInt("cityNum", cityNum);
            conf.setFloat("alpha", a);
            conf.setFloat("beta", b);
            conf.setFloat("rho", r);
            DefaultStringifier.storeArray(conf, list.toArray(),"distance");

            String jobName = tsp.class.getSimpleName();
            Job job = Job.getInstance(conf, jobName);



            //设置job运行的jar
            job.setJarByClass(tsp.class);

            //设置整个程序的输入
            FileInputFormat.setInputPaths(job, inputPath);

            //就是设置如何将输入文件解析成一行一行内容的解析类
            job.setInputFormatClass(TextInputFormat.class);


            //设置mapper
            job.setMapperClass(ACOMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

    //
            //设置整个程序的输出
            outputPath.getFileSystem(conf).delete(outputPath, true);//如果当前输出目录存在，删除之，以避免.FileAlreadyExistsException
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(TextOutputFormat.class);
    //
            //设置reducer
            job.setReducerClass(ACOReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
    //
            //指定程序有几个reducer去运行
            job.setNumReduceTasks(1);
            //提交程序
            job.waitForCompletion(true);
            index++;

        }

    }

}
