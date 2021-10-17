import java.io.*;
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class VaccineCount {
    public static class VaccineCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        // initialize the field variable
        private final static IntWritable one = new IntWritable(1);
        private final static int COLUMN_USED = 11;  //contains hashtags
        private final static String CSV_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException 
        {

            String[] columnData = value.toString().split(CSV_SEPARATOR);
            Pattern p = Pattern.compile("\\[[^\\[\\]]+\\]");

            for (String hashtags: columnData)
            {
                if(p.matcher(hashtags).matches()){
                    word.set(hashtags);
                    context.write(word, one);
                }
            }
        }
    }
    
    public static class VaccineCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
            Context context
            ) throws IOException, InterruptedException {
                int total = 0;
                for (IntWritable val : values) {
                total++ ;
            }
            context.write(key, new IntWritable(total));
        }
    }
    
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(VaccineCount.class);

        job.setMapperClass(VaccineCountMapper.class);
        job.setReducerClass(VaccineCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}