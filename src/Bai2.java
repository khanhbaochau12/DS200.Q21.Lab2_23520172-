import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lab2Statistics {

    public static class StatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Set<String> stopWords = new HashSet<>();
        private Text outKey = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());
            }
            br.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] cols = value.toString().split(";");
            if (cols.length < 5) return;

            String comment = cols[1].toLowerCase();
            String aspect = cols[2].trim();
            String category = cols[3].trim();

            if (!category.isEmpty()) {
                outKey.set("CATEGORY_" + category);
                context.write(outKey, one);
            }

            if (!aspect.isEmpty()) {
                outKey.set("ASPECT_" + aspect);
                context.write(outKey, one);
            }

            String[] words = comment.split("\\s+");
            for (String w : words) {
                w = w.replaceAll("[^a-zà-ỹ]", "");
                if (!w.isEmpty() && !stopWords.contains(w)) {
                    outKey.set("WORD_" + w);
                    context.write(outKey, one);
                }
            }
        }
    }

    public static class StatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            String keyStr = key.toString();
            if (keyStr.startsWith("WORD_")) {
                if (sum > 500) {
                    context.write(key, new IntWritable(sum));
                }
            } else {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai2");
        job.setJarByClass(Lab2Statistics.class);
        job.setMapperClass(StatMapper.class);
        job.setReducerClass(StatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
