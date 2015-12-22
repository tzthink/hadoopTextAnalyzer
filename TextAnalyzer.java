import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class TextAnalyzer11 {

  public static class Map extends Mapper<Object, Text, Text, Text>{

    static enum CountersEnum { INPUT_WORDS }

    private Text word = new Text();
	private Text vtext = new Text();
		
	private Set<String> patternsToSkip = new HashSet<String>();
		
	private long numRecords = 0;
	private String inputFile;

    private Configuration conf;
    private BufferedReader fis;
    
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();

      if (conf.getBoolean("textanalyzer11.skip.patterns", true)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
          Path patternsPath = new Path(patternsURI.getPath());
          String patternsFileName = patternsPath.getName().toString();
          parseSkipFile(patternsFileName);
        }
      }
    }

    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, " ");
      }

      
      String out_word = new String();
	  String vstring = new String();
	  HashMap<String, Integer> htest = new HashMap<String, Integer>();
			
	  int flagone = 1;
	  StringTokenizer out_tokenizer = new StringTokenizer(line);
	  StringTokenizer in_tokenizer = new StringTokenizer(line);
			
	  while (out_tokenizer.hasMoreTokens()) {
		  out_word = out_tokenizer.nextToken();
		  if (htest.containsKey(out_word)){
		  	continue;
		  }
		  else{
		  	htest.put(out_word, new Integer(1));
		  }
		  word.set(out_word);				
		  while(in_tokenizer.hasMoreTokens()){			
			  vstring = in_tokenizer.nextToken();
			  if(flagone == 1 && vstring == out_word){
				  flagone = 0;
				  continue;
			  }
			  vtext.set(vstring);
			  context.write(word, vtext);
			  Counter counter = context.getCounter(CountersEnum.class.getName(),
              	  CountersEnum.INPUT_WORDS.toString());
        	  counter.increment(1);
		  }
		  flagone = 1;
		  in_tokenizer = new StringTokenizer(line);
	  }
    }
  }

  public static class Reduce extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      HashMap<String, Integer> output_hashmap = new HashMap<String, Integer>();			
	  String output_string = new String();
	  int i;

	  for (Text val : values) {
			output_string = val.toString();
			if (output_hashmap.containsKey(output_string)){
				i = output_hashmap.get(output_string).intValue() + 1;
				output_hashmap.put(output_string, new Integer(i));
			}
			else{
				output_hashmap.put(output_string, new Integer(1));
			}
		}
			
		key.set(key.toString());
		String out_q = new String();
		String out_o = new String();
		String out_value = new String();
		out_value = "";
		Text output_text = new Text();
		Set<String> out_ss = output_hashmap.keySet();
		Iterator<String> out_is = out_ss.iterator();
		while (out_is.hasNext()){
			out_q = out_is.next();
			out_o = output_hashmap.get(out_q).toString();
			out_value = out_value + "<" + out_q + ", " + out_o + ">\n";
		}
		out_value = "\n" + out_value;	
		output_text.set(out_value);
			
		context.write(key, output_text);
	

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
      System.err.println("Usage: textanalyzer11 <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "text analyzer");
    job.setJarByClass(TextAnalyzer11.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("textanalyzer11.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}