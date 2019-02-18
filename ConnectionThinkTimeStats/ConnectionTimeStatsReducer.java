
// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ConnectionTimeStatsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

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
