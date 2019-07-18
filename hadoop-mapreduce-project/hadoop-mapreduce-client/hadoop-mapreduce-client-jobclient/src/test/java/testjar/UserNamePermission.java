/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package testjar;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public  class UserNamePermission      
{
  private static final Logger LOG =
      LoggerFactory.getLogger(UserNamePermission.class);

  //This mapper will read the user name and pass in to the reducer
  public static class UserNameMapper extends Mapper<LongWritable,Text,Text,Text>
  {
    Text key1 = new Text("UserName");
    public void map(LongWritable key, Text value, Context context)
      throws IOException,InterruptedException {
      Text val = new Text(System.getProperty("user.name").toString());
      context.write(key1, val);
    }
  }

  //The reducer is responsible for writing the user name to the file
  //which will be validated by the testcase
  public static class UserNameReducer extends Reducer<Text,Text,Text,Text>
  {
    public void reduce(Text key, Iterator<Text> values,
      Context context) throws IOException,InterruptedException {
	  			
      LOG.info("The key "+key);
      if(values.hasNext())
      {
        Text val = values.next();
        LOG.info("The value  "+val);
	  				 
        context.write(key,new Text(System.getProperty("user.name")));
	  }
	  				  			 
	}
  }
		
  public static void main(String [] args) throws Exception
  {
    Path outDir = new Path("output");
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "user name check"); 
			
			
    job.setJarByClass(UserNamePermission.class);
    job.setMapperClass(UserNamePermission.UserNameMapper.class);
    job.setCombinerClass(UserNamePermission.UserNameReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(UserNamePermission.UserNameReducer.class);
    job.setNumReduceTasks(1);
		    
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, outDir);
		    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

