/**
 * @author jiaxuehui
 * @version 1.0
 * @Description TODO
 * @Date 04/01/2019 4:04 PM
 */
import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class tsp {
    private static final Logger logger = Logger.getLogger(tsp.class.getName());


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();//Log4j是一个功能强大的日志组件,提供方便的日志记录，默认配置

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
        for (int i=0;i<cityNum;i++)
        {
            for (int j = 0;j<cityNum;j++){
                list.add(distanceWritable[i][j]);
                phe.add(new FloatWritable(0.1f));
            }
        }




        BufferedReader ants = new BufferedReader(new InputStreamReader(
                new FileInputStream("src/TSP/ants.txt")));
        int antNums = Integer.parseInt(ants.readLine());
        int group = Integer.parseInt(ants.readLine());
        System.out.print(group);
        int sum=0;
        ants.close();

        BufferedReader ACOargs = new BufferedReader(new InputStreamReader(new FileInputStream("src/TSP/ACOargs.txt")));
        int N = Integer.parseInt(ACOargs.readLine());
        float a = Float.parseFloat(ACOargs.readLine());
        float b = Float.parseFloat(ACOargs.readLine());
        float r = Float.parseFloat(ACOargs.readLine());
        ACOargs.close();

        //写信息素文件和距离文件和蚂蚁家族文件
        try
        {
            BufferedWriter antOut=new BufferedWriter(new FileWriter("src/TSP/antGroups.txt"));
            while (sum+group<antNums){
                antOut.write(group+"\n");
                sum+=group;
            }
            antOut.write(antNums-sum+"");
            antOut.close();
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }



        Path inputPath = new Path("src/TSP/antGroups.txt");

        int index = 0;
        while (index<N){  //N 次迭代
            Path outputPath = new Path("src/TSP/output"+index);
            Configuration conf = new Configuration();
            conf.setInt("cityNum", cityNum);
            conf.setFloat("alpha", a);
            conf.setFloat("beta", b);
            conf.setFloat("rho", r);
            DefaultStringifier.storeArray(conf, list.toArray(),"distance");
            DefaultStringifier.storeArray(conf, phe.toArray(),"pheromone");

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

    //map

    public static class ACOMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int cityNum;
        private IntWritable [][] distance;
        private FloatWritable [][] pheromone;
        private float alpha;
        private float beta;
        private float rho;
        private float[][] delta; //信息素变化矩阵，一个蚂蚁走过之后留下的信息素
        private List<Integer> tabu;
        private List<Integer> allowed;
        private int firstCity;
        private int currentCity;
        private int bestLength = Integer.MAX_VALUE;

        public void selectNextCity() { //根据信息素矩阵计算转移概率，选择下一个城市

            float[] p = new float[cityNum];  //概率数组
            float sum = 0.0f;
            // 计算分母部分
            for (Integer i : allowed) {
                sum += Math.pow(pheromone[currentCity][i.intValue()].get(), alpha)
                        * Math.pow(1.0 / distance[currentCity][i.intValue()].get(), beta);
            }
            // 计算概率矩阵
            for (int i = 0; i < cityNum; i++) {
                boolean flag = false;
                for (Integer j : allowed) {
                    if (i == j.intValue()) {
                        p[i] = (float) (Math.pow(pheromone[currentCity][i].get(), alpha) * Math
                                .pow(1.0 / distance[currentCity][i].get(), beta)) / sum;
                        flag = true;
                        break;
                    }


                }
                if (!flag) {
                    p[i] = 0.f;
                }
            }

            // 轮盘赌选择下一个城市
            Random random = new Random(System.nanoTime());
            float selectP = random.nextFloat();

            int selectCity = 0;
            float sum1 = 0.f;
            for (int i = 0; i < cityNum; i++) {
                sum1 += p[i];
                if (sum1 >= selectP) {
                    selectCity = i;
                    break;
                }
            }
            // 从允许选择的城市中去除select city
            for (Integer i : allowed) {
                if (i.intValue() == selectCity) {
                    allowed.remove(i);
                    break;
                }
            }
            // 在禁忌表中添加select city
            tabu.add(Integer.valueOf(selectCity));
            // 将当前城市改为选择的城市
            currentCity = selectCity;
        }
        


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //设置距离矩阵和蚁群参数
            super.setup(context);
            Configuration conf = context.getConfiguration();
            IntWritable [] dis= DefaultStringifier.loadArray(conf, "distance", IntWritable.class);
            FloatWritable [] phe= DefaultStringifier.loadArray(conf, "pheromone", FloatWritable.class);
            cityNum = conf.getInt("cityNum", 0);
            alpha = conf.getFloat("alpha", 0.0f);
            beta = conf.getFloat("beta", 0.0f);
            rho = conf.getFloat("rho", 0.0f);
            distance = new IntWritable[cityNum][cityNum];
            pheromone = new FloatWritable[cityNum][cityNum];
            for (int i=0 ; i<cityNum; i++){
                for (int j=0; j<cityNum; j++){
                    distance[i][j] = dis[i*cityNum+j];
                    pheromone[i][j] = phe[i*cityNum+j];
                }
            }
        }
            
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            //所有蚂蚁分为蚂蚁家族，避免一个map只进行一个蚂蚁的寻路，降低效率
            int groupNum = Integer.parseInt(v1.toString());//蚂蚁家族里蚂蚁数量
            bestLength = Integer.MAX_VALUE;
            String res = new String();
            
            tabu = new ArrayList<Integer>();
            allowed = new ArrayList<Integer>();
            for (int antI = 0; antI<groupNum; antI++){//蚂蚁家族中所有蚂蚁完成寻路
                //初始化禁忌表和允许访问表
                delta = new float[cityNum][cityNum];
                tabu.clear();//禁忌表最初是空表
                for (int x = 0; x < cityNum; x++) {
                    Integer integer = new Integer(x);
                    allowed.add(integer);  //将所有城市加入允许访问表
                    for (int y = 0; y < cityNum; y++) {
                        delta[x][y] = 0.f;
                    }
                }
                
                //随机选一个起始城市
                Random random = new Random(System.nanoTime());
                firstCity = random.nextInt(cityNum);
                currentCity = firstCity;
                tabu.add(firstCity);//加入禁忌表
                // 允许表移除起始城市
                for (Integer it : allowed) {
                    if (it.intValue() == firstCity) {
                        allowed.remove(it);
                        break;
                    }
                }


                //蚂蚁走遍所有的城市
                for (int j=0;j<cityNum-1;j++){
                    selectNextCity();//选一个城市走
                }
                tabu.add(firstCity);//将第一个城市加入禁忌表，成环，此时禁忌表中的顺序就是便利顺序

                //计算该蚂蚁的路径长度
                int len = 0;
                res="";
                for (int i = 0; i < cityNum; i++) {
                    res=res.concat(tabu.get(i)+" ");
                    len += distance[tabu.get(i)][this.tabu.get(i + 1)].get();
                }
                if(len<bestLength)
                    bestLength=len;
                res=res.concat(tabu.get(cityNum)+"|"+bestLength);
            }
            context.write(new Text(res), new Text(""));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    // reduce
    public static class ACOReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {

            context.write(k2, new Text(""));
        }
    }


}
