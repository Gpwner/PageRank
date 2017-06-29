package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 对pr值进行重计算,每个pr都除以pr总值
 */
public class Standardization {

    public static class FinallyResultMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable
            , Text, Text, Text> {

        Text k = new Text("finally");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            System.out.println("Standardization input:");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            context.write(k, value);
        }
    }

    public static class FinallyResultReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("Standardization input:");
            StringBuilder printStr = new StringBuilder();
            float totalPr = 0f;
            List<String> list = new ArrayList<String>();
            for (Text value : values) {
                String valueStr = value.toString();
                list.add(valueStr);

                String[] strArr = HadoopUtils.SPARATOR.split(valueStr);
                totalPr += Float.parseFloat(strArr[1]);

                printStr.append(",").append(valueStr);
            }
            System.out.println(printStr.toString().replace(",", ""));

            for (String s : list) {
                String[] strArr = HadoopUtils.SPARATOR.split(s);
                k.set(strArr[0]);
                v.set(String.valueOf(Float.parseFloat(strArr[1]) / totalPr));
                context.write(k, v);
                System.out.println("Standardization output:");
                System.out.println(k.toString() + ":" + v.toString());
                System.out.println();
            }
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath = "/pagerank/pagerank.csv";
        String outPath = "/pagerank/finally-result";
        
        Job job = Job.getInstance(conf, "AdjacencyMatrix");
        HDFSUtils hdfs = new HDFSUtils(conf);
		hdfs.deleteDir(outPath);
		job.setJarByClass(Standardization.class);
		job.setMapperClass(FinallyResultMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FinallyResultReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		job.waitForCompletion(true);
    }
    
    public static void main(String[] args) throws Exception{
		Standardization.run();
	}
}
