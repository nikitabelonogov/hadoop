import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nikitabelonogov on 20.04.16.
 */

public class mapreduce {

    public static class MyMapper extends TableMapper<Text, IntWritable> {

        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String val = new String(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("attr")));
            text.set(val);

            context.write(text, ONE);
        }
    }

    public static class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(i));

            context.write(null, put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config,"ExampleSummary");
        job.setJarByClass(mapreduce.class);     // class that contains mapper and reducer

        List scans = new ArrayList();

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("oid"));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "in".getBytes());
        scans.add(scan);

        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(
                scans,               // Scan instance to control CF and attribute selection
                MyMapper.class,     // mapper class
                Text.class,         // mapper output key
                IntWritable.class,  // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                "out",        // output table
                MyReducer.class,    // reducer class
                job);
        job.setNumReduceTasks(1);   // at least one, adjust as required

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}