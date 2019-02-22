import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;

/**
 * @author jiaxuehui
 * @version 1.0
 * @Description TODO
 * @Date 10/02/2019 6:45 PM
 */
public class ACOReducer extends Reducer<Text, Text, Text, Text> {
    private int cityNum;
    private float rho;
    private float[][] phe;


    protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        cityNum = conf.getInt("cityNum", 0);
        rho = conf.getFloat("rho", 0.0f);

        phe = new float[cityNum][cityNum];
        FileSystem fs = FileSystem.get(URI.create("/TSP/Pheromone.txt"),conf);
        FSDataInputStream fsr = fs.open(new Path("/TSP/Pheromone.txt"));
        BufferedReader pheR = new BufferedReader(new InputStreamReader(fsr));
        String[] line;
        for (int i=0 ; i<cityNum; i++){
            line = pheR.readLine().split(" ");
            for (int j=0; j<cityNum; j++){
                phe[i][j] = Float.parseFloat(line[j]);
            }
        }
        pheR.close();

        String[] res = k2.toString().split("\\|");
        int bestL = Integer.parseInt(res[1]);
        String[] path = res[0].split(" ");
        // 信息素挥发
        for (int i = 0; i < cityNum; i++)
            for (int j = 0; j < cityNum; j++)
                phe[i][j]*=(1 - rho);
        // 信息素更新

        // 更新这只蚂蚁的信息数变化矩阵，对称矩阵

        for (int j = 0; j < cityNum; j++) {
            phe[Integer.parseInt(path[j])][Integer.parseInt(path[j+1])]=phe[Integer.parseInt(path[j])][Integer.parseInt(path[j+1])]+(float)(1./bestL);
            phe[Integer.parseInt(path[j+1])][Integer.parseInt(path[j])]=phe[Integer.parseInt(path[j+1])][Integer.parseInt(path[j])]+(float)(1./bestL);
        }

        Path PheromoneFile = new Path("/TSP/Pheromone.txt");
        FileSystem fs2 = FileSystem.get(conf);
        if(fs2.exists(PheromoneFile)){
            fs2.delete(PheromoneFile,true);
        }
        FSDataOutputStream fso2 = fs2.create(PheromoneFile);

        BufferedWriter results=new BufferedWriter(new OutputStreamWriter(fso2));
        for (int i=0 ; i<cityNum; i++){
            for (int j=0; j<cityNum; j++){
                results.write(phe[i][j]+" ");
            }
            results.write("\n");
        }
        results.close();
        context.write(new Text("10"), new Text("|"+k2.toString()));
    }
}