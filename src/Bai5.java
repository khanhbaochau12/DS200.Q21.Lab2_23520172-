import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lab5Analytics {
    public static class AnalyticsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] cols = value.toString().split(";");
            if (cols.length < 5) return;
            String aspect = cols[2].trim();
            String sentiment = cols[4].trim();
            if (!aspect.isEmpty() && !sentiment.isEmpty()) {
                context.write(new Text(aspect), new Text(sentiment));
            }
        }
    }

    public static class AnalyticsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int pos = 0, neg = 0;
            for (Text val : values) {
                if (val.toString().equalsIgnoreCase("positive")) pos++;
                else if (val.toString().equalsIgnoreCase("negative")) neg++;
            }
            int total = pos + neg;
            double posRate = (total > 0) ? (pos * 100.0 / total) : 0;
            context.write(key, new Text(String.format("Total: %d | Satisfaction: %.2f%%", total, posRate)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai5");
        job.setJarByClass(Lab5Analytics.class);
        job.setMapperClass(AnalyticsMapper.class);
        job.setReducerClass(AnalyticsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
