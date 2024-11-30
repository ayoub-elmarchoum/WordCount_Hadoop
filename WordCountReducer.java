import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
// Define the WordCountReducer class, extending the Hadoop Reducer class
// This Reducer will take input key-value pairs (word, list of counts)
// and output the aggregated count for each word
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Instance variable to hold the final count for a word

    private IntWritable result = new IntWritable();  // Résultat final (comptage des mots)

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0; // Initialize a counter to sum the counts for the current word

       // Iterate through the list of counts for the current word
        for (IntWritable val : values) {
            sum += val.get();
        }

      // Set the final count to the result variable
        result.set(sum);
  // Write the word and its total count to the context (output of the Reducer)
        context.write(key, result);  // Émet le mot et son total
    }
}
