
// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowAvgDurReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		long i = 0;
		double timeDiffTotal = 0;
		double prevTime = 0;
		double temp = 0;

		// iterate through all the values (count == 1) with a common key
		for (DoubleWritable value : values) {
			if (i == 0) {
				prevTime = value.get();
				i++;
			} else {
				temp = value.get();
				timeDiffTotal += (temp - prevTime);

				i++;
				prevTime = temp;
			}
		}
		
		i = i - 1;

		if (i == 0) {
			context.write(key, new DoubleWritable(0));

		} else {
			context.write(key, new DoubleWritable(timeDiffTotal / i));
		}
	}
}
