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

public class ConnectionTimeCountMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\s");

        String IPaddr1 = new String();
        String IPaddr2 = new String();

        int last_dot;
        int last_dot2;
        char direction;
        double thinkTime;

        // checks
        if(tokens.length == 8) {


            // get the direction of sending arrow
            direction = tokens[3].charAt(0);

            // get IP address 1
            IPaddr1 = tokens[2];
            last_dot = IPaddr1.lastIndexOf('.');
            // get IP address 1 port
            IPaddr1 = IPaddr1.substring(last_dot + 1, IPaddr1.length() - 1);
            // get IP address 2
            IPaddr2 = tokens[4];
            last_dot2 = IPaddr2.lastIndexOf('.');
            // get IP address 2 port
            IPaddr2 = IPaddr2.substring(last_dot2 + 1, IPaddr2.length() - 1);

            // get thinktime
            thinkTime = Double.parseDouble(tokens[7]);

            if (direction == '>') {
                context.write(new Text(IPaddr1), new DoubleWritable(thinkTime));
            } else {
                context.write(new Text(IPaddr2), new DoubleWritable(thinkTime));
            }
        }
    }
}
