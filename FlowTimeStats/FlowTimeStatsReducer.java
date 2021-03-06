
// reducer function for application to compute the mean and
// standard deviation for the number of bytes sent by
// each IP address in the IP address pair that defines
// a flow.  Compute results only for flows that send 10
// or more ADUs in both directions.  

import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowTimeStatsReducer extends Reducer<Text, DoubleWritable, Text, Text> {

	// define ArrayLists to cache byte counts from each of the ADUs
	// in a flow. This local cacheing is necessary because the
	// Iterable provided by the MapReduce framework cannot be
	// iterated more than once. bytes1_2 caches byte counts sent
	// from IP address 1 to 2 and bytes2_1 caches those in the
	// opposite direction.

	private ArrayList<Float> thinkTimes = new ArrayList<Float>();

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

		float sum = 0;
		float count = 0;
		float thinkTime = 0;

		// clear the cache for each new flow key
		thinkTimes.clear();

		// iterate through all the values with a common key
		for (DoubleWritable value : values) {
			thinkTime = (float) value.get();

			count++;
			sum += thinkTime;
			thinkTimes.add(thinkTime); // cache think time value
		}

		if (count >= 10) {
			// calculate the mean
			float mean = sum / count;

			// calculate standard deviation
			float sumOfSquares = 0.0f;

			// compute sum of square differences from mean
			for (Float f : thinkTimes) {
				sumOfSquares += (f - mean) * (f - mean);
			}

			// compute the variance and take the square root to get standard deviation
			float stddev = (float) Math.sqrt(sumOfSquares / (count - 1));

			// output byte mean and standard deviation for both IP addresses
			String flowStats = Float.toString(mean) + " " + Float.toString(stddev);

			context.write(key, new Text(flowStats));
		}
	}
}
