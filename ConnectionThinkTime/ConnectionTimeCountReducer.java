
// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ConnectionTimeCountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		double timeTotal = 0;

		// iterate through all the values (count == 1) with a common key
		for (DoubleWritable value : values) {
			timeTotal += value.get();
		}

		context.write(key, new DoubleWritable(timeTotal));
	}
}
