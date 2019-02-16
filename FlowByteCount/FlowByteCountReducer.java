
// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowByteCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		long count = 0;
		long byteTotal = 0;

		// iterate through all the values (count == 1) with a common key
		for (IntWritable value : values) {
			byteTotal = byteTotal + value.get();
		}

		context.write(key, new LongWritable(byteTotal));
	}
}
