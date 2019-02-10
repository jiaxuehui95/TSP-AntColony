import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author jiaxuehui
 * @version 1.0
 * @Description TODO
 * @Date 10/02/2019 6:44 PM
 */

public class ACOMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger logger = Logger.getLogger(tsp.class.getName());

    private int cityNum;
    private IntWritable[][] distance;
    private float [][] pheromone;
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
            sum += Math.pow(pheromone[currentCity][i.intValue()], alpha)
                    * Math.pow(1.0 / distance[currentCity][i.intValue()].get(), beta);
        }
        // 计算概率矩阵
        for (int i = 0; i < cityNum; i++) {
            boolean flag = false;
            for (Integer j : allowed) {
                if (i == j.intValue()) {
                    p[i] = (float) (Math.pow(pheromone[currentCity][i], alpha) * Math
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


        cityNum = conf.getInt("cityNum", 0);
        alpha = conf.getFloat("alpha", 0.0f);
        beta = conf.getFloat("beta", 0.0f);
        rho = conf.getFloat("rho", 0.0f);
        distance = new IntWritable[cityNum][cityNum];
        pheromone = new float[cityNum][cityNum];
        BufferedReader pheR = new BufferedReader(new InputStreamReader(new FileInputStream("src/TSP/Pheromone.txt")));
        String[] line;
        for (int i=0 ; i<cityNum; i++){
            line = pheR.readLine().split(" ");
            for (int j=0; j<cityNum; j++){
                distance[i][j] = dis[i*cityNum+j];
                pheromone[i][j] = Float.parseFloat(line[j]);
            }
        }
        pheR.close();
    }

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

        String[] v1s = v1.toString().split("\\|");
        String bestPath="";
        //所有蚂蚁分为蚂蚁家族，避免一个map只进行一个蚂蚁的寻路，降低效率
        int groupNum = Integer.parseInt(v1s[0].trim());//蚂蚁家族里蚂蚁数量
        if (v1s.length<3) {
            bestLength = Integer.MAX_VALUE;
        }
        else {
            bestLength = Integer.parseInt(v1s[2]);
            bestPath = v1s[1];
        }
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
            if(len<bestLength){
                bestLength=len;
                res = res.concat(tabu.get(cityNum).toString());
                bestPath = res;
                res=res+"|"+bestLength;

            }
            else
            {
                res = bestPath+"|"+bestLength;
            }
        }
        //写信息素文件和距离文件和蚂蚁家族文件
        try
        {
            BufferedWriter results=new BufferedWriter(new FileWriter("src/TSP/res.txt",true));

            results.write(bestLength+"\n");
            results.close();
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        context.write(new Text(res), new Text(""));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}