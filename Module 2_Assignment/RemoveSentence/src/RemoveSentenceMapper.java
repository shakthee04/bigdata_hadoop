import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
/**
 * @author M1032938
 *
 */
public class RemoveSentenceMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		valueing value = value.tovalueing();
		for (valueing sentence : value.split("\n")) {
			if (sentence.length() > 15) {
				output.collect(new Text(sentence+"\t Length of Sentence:"), new IntWritable(sentence.length()));
			}
		}
		
		
	}

}
