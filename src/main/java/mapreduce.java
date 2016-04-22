import com.google.common.collect.Iterables;
import org.apache.batik.css.engine.value.Value;
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
import org.apache.hadoop.hbase.mapreduce.TableSplit;
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

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {
        @Override
        public void map(ImmutableBytesWritable row, Result result, Context context) throws IOException, InterruptedException {
            byte[] tableName = ((TableSplit) context.getInputSplit()).getTableName();
            if(Bytes.compareTo(tableName, Bytes.toBytes("authors")) == 0) {
                ImmutableBytesWritable author = new ImmutableBytesWritable(row.get());
                Put put = new Put(Bytes.toBytes("homeland"));
                put.addColumn(Bytes.toBytes("homeland"), null, result.getValue(Bytes.toBytes("homeland"), null));
                context.write(author, put);
            }
            else if(Bytes.compareTo(tableName, Bytes.toBytes("books")) == 0) {
                ImmutableBytesWritable author = new ImmutableBytesWritable(result.getValue(Bytes.toBytes("author"), null));
                Put put = new Put(Bytes.toBytes("book"));
                put.addColumn(Bytes.toBytes("book"), null, row.get());
                context.write(author, put);
            }
        }
    }

    public static class MyReducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {
        @Override
        public void reduce(ImmutableBytesWritable author, Iterable<Put> values, Context context) throws IOException, InterruptedException {

            List<Put> books = new ArrayList<Put>();
            Put homeland = new Put(author.get());

            for (Put val : values) {
                if (Bytes.compareTo(val.getRow(), Bytes.toBytes("book")) == 0)
                    books.add(val);
                if (Bytes.compareTo(val.getRow(), Bytes.toBytes("homeland")) == 0)
                    homeland = val;
            }

            Put resultput = new Put(author.get());
            Cell kv = homeland.get(Bytes.toBytes("homeland"), null).get(0);
            resultput.addColumn(Bytes.toBytes("homeland"), null, Bytes.copy(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
            resultput.addColumn(Bytes.toBytes("book quantity"), null, Bytes.toBytes(books.size()));

            context.write(null, resultput);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config,"Summary Join");
        job.setJarByClass(mapreduce.class);

        List scans = new ArrayList();

        Scan books = new Scan();
        books.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("books"));
        books.addFamily(Bytes.toBytes("author"));
        scans.add(books);

        Scan autors = new Scan();
        autors.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("authors"));
        autors.addFamily(Bytes.toBytes("homeland"));
        scans.add(autors);

        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("out", MyReducer.class, job);
        job.setNumReduceTasks(1);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}