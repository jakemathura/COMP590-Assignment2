
// map function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowTimeStatsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split("\\s");

		String IPaddr1 = new String();
		String IPaddr2 = new String();
		String finalFlow = new String();

		int last_dot;
		char direction;
		double thinkTime;

		// checks
		if (tokens.length == 8) {

			// get the direction of sending arrow
			direction = tokens[3].charAt(0);

			// get IP address 1
			IPaddr1 = tokens[2];
			last_dot = IPaddr1.lastIndexOf('.');
			IPaddr1 = IPaddr1.substring(0, last_dot);
			// get IP address 2
			IPaddr2 = tokens[4];
			last_dot = IPaddr2.lastIndexOf('.');
			IPaddr2 = IPaddr2.substring(0, last_dot);

			// get bytes
			thinkTime = Double.parseDouble(tokens[7]);

			if (direction == '>') {
				finalFlow = IPaddr1 + " " + IPaddr2;
				context.write(new Text(finalFlow), new DoubleWritable(thinkTime));
			} else {
				finalFlow = IPaddr2 + " " + IPaddr1;
				context.write(new Text(finalFlow), new DoubleWritable(thinkTime));
			}
		}
	}
}
