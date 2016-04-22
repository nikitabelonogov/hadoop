import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
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

        private Text key = new Text();
        private IntWritable val = new IntWritable(0);

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            key.set(Bytes.toString(row.get()));
            val.set(Integer.parseInt(new String(value.getValue(Bytes.toBytes("key"), null))));

            context.write(key, val);
        }
    }

    public static class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("key"), null, Bytes.toBytes(i));

            context.write(null, put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config,"Summary Join");
        job.setJarByClass(mapreduce.class);

        List scans = new ArrayList();

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("key"));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("in1"));
        scans.add(scan);

        Scan scan2 = new Scan();
        scan2.addFamily(Bytes.toBytes("key"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("in2"));
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("out", MyReducer.class, job);
        job.setNumReduceTasks(1);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}