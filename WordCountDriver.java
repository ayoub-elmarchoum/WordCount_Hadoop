import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

    public static void main(String[] args) throws Exception {

                // Check if the correct number of arguments is provided (input and output paths)

        if (args.length != 2) {
            System.err.println("Usage: WordCountDriver <input path> <output path>");
            System.exit(-1);
        }

         // Create a new Hadoop configuration object
        Configuration conf = new Configuration();
        // Create a new MapReduce job instance with the configuration and job name
        Job job = Job.getInstance(conf, "Word Count");

        // Set the Jar file that contains the driver, mapper, and reducer classes
        job.setJarByClass(WordCountDriver.class);

        // Specify the Mapper and Reducer classes for the job
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

       // Specify the output key and value types for the Mapper and Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

              // Set the input and output paths for the job

        FileInputFormat.addInputPath(job, new Path(args[0]));// Input path (HDFS or local)
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path (HDFS or local)

        // Submit the job to the cluster and wait for it to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
