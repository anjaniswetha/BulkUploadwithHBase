package com.sample.bulkload.hbase;  
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
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class HBaseBulkLoad {  
	public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
			String line = value.toString(); 
			String s = line.toString().replaceAll("[^a-zA-Z0-9=\"]+", " ")
					.replace("row", "").replaceAll("\"", "\'")
					.replaceAll("'", "\\\\'").trim();


			StringTokenizer st = new StringTokenizer(s, "\'");
			String rowId = st.nextToken().replaceAll("\\\\", "")
					.replaceAll("=", "").trim();
			String rowKey = st.nextToken().replace("\\", "").trim();

			Put HPut = new Put(Bytes.toBytes(rowKey));  
			ImmutableBytesWritable HKey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
			while(st.hasMoreTokens())
			{
				String colname = st.nextToken().replaceAll("\\\\", "")
						.replaceAll("=", "").trim();
				String valname = st.nextToken().replace("\\", "").trim();
				if(colname.equals("CreationDate") || colname.equals("LastEditDate") || colname.equals("LastActivityDate")
						|| colname.equals("OwnerUserId") || colname.equals("LastEditorUserId"))
				{
					HPut.add(Bytes.toBytes("User"), Bytes.toBytes(colname), Bytes.toBytes(valname));  

				}
				else 
				{
					HPut.add(Bytes.toBytes("Post"), Bytes.toBytes(colname), Bytes.toBytes(valname));  
				}


			}
			context.write(HKey,HPut); 
		}   
	}  
	public static void main(String[] args) throws Exception {  
		Configuration conf = HBaseConfiguration.create();  
		String inputPath = args[0];  
		String outputPath = args[1];  
		HTable hTable = new HTable(conf, "bigd24-hbase-sample");  
		Job job = new Job(conf,"HBase_Bulk_loader");        
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
		job.setMapOutputValueClass(Put.class);  
		job.setSpeculativeExecution(false);  
		job.setReduceSpeculativeExecution(false);  
		job.setInputFormatClass(TextInputFormat.class);  
		job.setOutputFormatClass(HFileOutputFormat.class);  
		job.setJarByClass(HBaseBulkLoad.class);  
		job.setMapperClass(HBaseBulkLoad.BulkLoadMap.class);  
		FileInputFormat.setInputPaths(job, inputPath);  
		FileOutputFormat.setOutputPath(job,new Path(outputPath));             
		HFileOutputFormat.configureIncrementalLoad(job, hTable);  
		System.exit(job.waitForCompletion(true) ? 0 : 1);  
	}  
}  
