import java.io.IOException;  
import java.util.HashMap;  
import java.util.Iterator;  
import java.util.Map;  
import java.util.StringTokenizer;
import java.util.Map.Entry;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;  
import org.apache.hadoop.hbase.util.Bytes;  
import org.apache.hadoop.io.FloatWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageCalculator {
	
     static class Mapper1 extends TableMapper<Text, IntWritable> {

	        private int numRecords = 0;
	        private Text word = new Text();
	       
            public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException,InterruptedException {
	        	 
	                String val = new String(values.getValue(Bytes.toBytes("Post"), Bytes.toBytes("AnswerCount")));
	                      
	        		if (val != null){
	        			word.set("Avg");
		                context.write(word, new IntWritable(Integer.parseInt(val)));
	        		}
	        		
	        }	                
	                
	    }
      public static class Reducer1 extends
	      Reducer<Text, IntWritable, Text, FloatWritable> {

            public void reduce(Text key, Iterable<IntWritable> values,
			    Context context) throws IOException, InterruptedException {
		        float sum = 0;
		        float count = 0;
	           	float avg = 0;
	           	
		        for (IntWritable val : values) {
			      sum += val.get();
			      count = count + 1;
		        }
		    	avg = sum / count;
			   context.write(key, new FloatWritable(avg));
            }
      }
	
      public static void main(String[] args) throws Exception {
		    Configuration con = new Configuration();
            String[] otherArgs = new GenericOptionsParser(con, args)
					.getRemainingArgs();
	        HBaseConfiguration conf = new HBaseConfiguration();
	        Job job = new Job(conf, "AverageCalc");
	        job.setJarByClass(AverageCalculator.class);
	        Scan scan = new Scan();
	        scan.setCaching(500);
	        scan.setCacheBlocks(false); 
	        scan.addFamily(Bytes.toBytes("Post"));
	        FilterList li = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("Post"),Bytes.toBytes("PostTypeId"),
	        	CompareOp.EQUAL, Bytes.toBytes("1"));       
	        li.addFilter(filter);
	        scan.setFilter(li);
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
	        job.setOutputKeyClass(Text.class);
	        TableMapReduceUtil.initTableMapperJob("bigd24-hbase-sample", scan, Mapper1.class, Text.class,
	        		IntWritable.class, job);
	        job.setReducerClass(Reducer1.class);
			job.setOutputValueClass(FloatWritable.class);
	        System.exit(job.waitForCompletion(true) ? 0 : 1);

	    }

}

