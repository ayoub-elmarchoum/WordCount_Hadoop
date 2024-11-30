import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);  // Valeur à chaque occurrence du mot
    // Instance variable to hold a word

    private Text word = new Text();  // Objet représentant un mot

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
               // Convert the input line (value) from Text to a Java String
        String line = value.toString();

        // Split the line into words using whitespace as the delimiter
        String[] words = line.split("\\s+");

       // Iterate over each word in the array
 // Set the current word as the key
        for (String w : words) {
            word.set(w);  // Définir le mot comme clé

// Write the output as a key-value pair (word, 1)
            context.write(word, one);  // Émettre le mot avec la valeur 1
        }
    }
}
