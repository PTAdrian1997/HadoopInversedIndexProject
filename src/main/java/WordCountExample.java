import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountExample {

    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),
                    "\"\',.()?![]#$*+-;:_/\\<>@%& ");
            while (itr.hasMoreElements()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        job1 job1 = job1.getInstance(conf, "word count");
//        job1.setJarByClass(WordCountExample.class);
//        job1.setMapperClass(TokenizerMapper.class);
//        job1.setCombinerClass(IntSumReducer.class);
//        job1.setReducerClass(IntSumReducer.class);
//        job1.setOutputKeyClass(Text.class);
//        job1.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
//    }

}