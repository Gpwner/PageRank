package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * 将邻接概率矩阵和pr矩阵进行计算并将得到的pr结果输出
 */
public class CalcPageRank {

    /**
     * 输入邻接概率矩阵和pr矩阵
     * 按照矩阵相乘的公式,将对应的数据输出到reduce进行计算
     */
    public static class CalcPeopleRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();
        String flag = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            flag = fileSplit.getPath().getName();
            System.out.println("CalcPeopleRankMapper input type:");
            System.out.println(flag);
        }

        /**
         * k的作用是将pr矩阵的列和邻接矩阵的行对应起来
         * 如:pr矩阵的第一列要和邻接矩阵的第一行相乘,所以需要同时输入到reduce中
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            System.out.println(value.toString());
            int nums = 4;
            //处理pr矩阵
            if (flag.startsWith("pagerank")) {
                String[] strArr = HadoopUtils.SPARATOR.split(value.toString());
                //第一位为用户id,输入的每行内容都为pr矩阵中的一列,所以也可以看成是列数

                for (int i = 1; i <= nums; i++) {
                    k.set(String.valueOf(i));
                    //pr为标识符,i为该列中第i行,strArr[1]为值
                    v.set("pr:" + strArr[0] + "," + strArr[1]);
                    context.write(k, v);
                }
            }
            //处理邻接概率矩阵
            else {
                String[] strArr = HadoopUtils.SPARATOR.split(value.toString());
                //k为用户id,输入的每行就是邻接概率矩阵中的一行,所以也可以看成行号
                System.out.println("strArr.length "   +strArr.length);
                for (int i = 1; i < strArr.length; i++) {
                    k.set(String.valueOf(i));
                    //matrix为标识符,i为该行中第i列,strArr[i]为值
                    v.set("matrix:" + strArr[0] + "," + strArr[i]);
                    context.write(k, v);
                }
            }
        }
    }

    /**
     * 每行输入都是两个矩阵相乘中对应的值
     * 如:邻接矩阵的第一行的值和pr矩阵第一列的值
     */
    public static class CalcPeopleRankReducer extends Reducer<Text, Text, Text, Text> {



        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("CalcPeopleRankReducer input:");
            StringBuilder printStr = new StringBuilder();

            Text v = new Text();
            //阻尼系数
            int nums = 4;
            //阻尼系数
            float d = 0.85f;

            //pr统计
            float pr = 0f;
            //存储pr矩阵列的值
            Map<Integer, Float> prMap = new HashMap<Integer, Float>();
            //存储邻接矩阵行的值
            Map<Integer, Float> matrixMap = new HashMap<Integer, Float>();
            //将两个矩阵对应的值存入对应的map中
            for (Text value : values) {
                String valueStr = value.toString();
                String[] kv = HadoopUtils.SPARATOR.split(valueStr.split(":")[1]);
                if (valueStr.startsWith("pr")) {
                    prMap.put(Integer.parseInt(kv[0]), Float.valueOf(kv[1]));
                } else {
                    matrixMap.put(Integer.parseInt(kv[0]), Float.valueOf(kv[1]));
                }
                printStr.append(",").append(valueStr);
            }
            System.out.println(printStr.toString().replaceFirst(",", ""));
            //根据map中的数据进行计算
            for (Map.Entry<Integer, Float> entry : matrixMap.entrySet()) {
                pr += entry.getValue() * prMap.get(entry.getKey());
            }
            pr = pr * d + (1 - d) / nums;
            v.set(String.valueOf(pr));
            System.out.println("CalcPeopleRankReducer output:");
            System.out.println(key.toString() + ":" + v.toString());
            System.out.println();
            context.write(key, v);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath1 = "/pagerank/probility-matrix/part-*";
        String inPath2 = "/pagerank/pagerank.csv";
        String outPath = "/pagerank/pr/";
        Job job = Job.getInstance(conf, "AdjacencyMatrix");
        HDFSUtils hdfs = new HDFSUtils(conf);
		hdfs.deleteDir(outPath);
		job.setJarByClass(CalcPageRank.class);
		job.setMapperClass(CalcPeopleRankMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(CalcPeopleRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inPath1));
		FileInputFormat.addInputPath(job, new Path(inPath2));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		job.waitForCompletion(true);

        hdfs.deleteDir(inPath2);
        hdfs.rename(outPath + "/part-r-00000", inPath2);
    }
    
    public static void main(String[] args) throws Exception{
		CalcPageRank.run();
	}
}
