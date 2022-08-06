package uk.ac.man.cs.comp38211.exercise;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import uk.ac.man.cs.comp38211.io.array.ArrayListOfLongsWritable;
import uk.ac.man.cs.comp38211.io.array.ArrayListWritable;
import uk.ac.man.cs.comp38211.io.pair.Pair;
import uk.ac.man.cs.comp38211.io.pair.PairOfStrings;
import uk.ac.man.cs.comp38211.io.pair.PairOfTextArrayListOfLongs;
import uk.ac.man.cs.comp38211.io.pair.PairOfWritables;
import uk.ac.man.cs.comp38211.ir.Stemmer;
import uk.ac.man.cs.comp38211.ir.StopAnalyser;
import uk.ac.man.cs.comp38211.util.XParser;
import uk.ac.man.cs.comp38211.util.array.ArrayListOfLongs;

public class BasicInvertedIndex extends Configured implements Tool
{
    private static final Logger LOG = Logger
            .getLogger(BasicInvertedIndex.class);

    public static class Map extends 
            Mapper<Object, Text, Text, PairOfWritables<Text,ArrayListWritable<LongWritable>>>
    {

        // INPUTFILE holds the name of the current file
        private final static Text INPUTFILE = new Text();
        
        // TOKEN should be set to the current token rather than creating a 
        // new Text object for each one
        @SuppressWarnings("unused")
        private final static Text TOKEN = new Text();

        // The StopAnalyser class helps remove stop words
        @SuppressWarnings("unused")
        private StopAnalyser stopAnalyser = new StopAnalyser();
        
        private Pair term_frequency = new Pair();
        
        // Cache help us to remove duplicates
        private HashMap<String, ArrayListWritable<LongWritable>> cache = new HashMap<String, ArrayListWritable<LongWritable>>();
        private LongWritable token_number = new LongWritable();
        
        // The stem method wraps the functionality of the Stemmer
        // class, which trims extra characters from English words
        // Please refer to the Stemmer class for more comments
        @SuppressWarnings("unused")
        private String stem(String word)
        {
            Stemmer s = new Stemmer();

            // A char[] word is added to the stemmer with its length,
            // then stemmed
            s.add(word.toCharArray(), word.length());
            s.stem();

            // return the stemmed char[] word as a string
            return s.toString();
        }
        
        // This method gets the name of the file the current Mapper is working
        // on
        @Override
        public void setup(Context context)
        {
            String inputFilePath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String[] pathComponents = inputFilePath.split("/");
            INPUTFILE.set(pathComponents[pathComponents.length - 1]);
        }
        
        private String remove_reference(String word)
        {
        	int reference_start,reference_end;
        	String substring;
        	while (word.indexOf(']') != -1 && word.indexOf('[') != -1)
        	{
        		reference_start = word.indexOf('[');
        		reference_end = word.indexOf(']');
        		//if(Character.isDigit(word.charAt(reference_start+1)) == false && word.charAt(reference_start+1) != '.')
        		//{
        			//break;
        		//}
        		substring = word.substring(reference_start,reference_end+1);
        		word = word.replace(word.substring(reference_start,reference_end+1),"");
        	}
        	return word;
        }
        
        private boolean end(char c)
        {
        	if(c == '.' || c == '?' || c == '!' || c == ';')
        	{
        		return true;
        	}
        	return false;
        }
        
        private String removeEnd(String sentence)
        {
        	while(!sentence.equals("") && end(sentence.charAt(sentence.length() - 1)))
        	{
        		sentence = sentence.substring(0,sentence.length() - 1);
        	}
        	return sentence;
        }
         
        // TODO
        // This Mapper should read in a line, convert it to a set of tokens
        // and output each token with the name of the file it was found in
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
        	// The line is broken up and turned into an iterator
            String line = value.toString();
            
            long term_count = 0;
            
            //Number representing the difference number of opened and closed quotes
            int quote_number = 0;
            String quote = "";
            boolean in_quote = false;
            String quote_token = "";
            
            String name_cache = "";
            boolean in_name = false;
            int name_length = 0;
            
            String temp_String = "";
            
            boolean firstWord = true;
            
            StringTokenizer itr = new StringTokenizer(line);
            
            // Finds the file name and saves it in the variable INPUTFILE
            setup(context);
            
            // While there are more tokens in the input, output the file name
            while (itr.hasMoreTokens())
            {
            	String token = itr.nextToken();
            	PrintWriter writer = new PrintWriter("a.txt","UTF-8");
            	
            	
            	token = remove_reference(token);
            	token = token.replaceAll("\\(|\\,|\\)", "");
            	
            	//////////////////////////////////Quote
            	temp_String = token;
            	while (temp_String.indexOf('"') != -1)
            	{
            		in_quote = true;
            		if(temp_String.indexOf('"') == 0)
            		{
            			quote_number += 1;
            			temp_String = temp_String.substring(1);
            			if(quote_number == 1)
            			{
            				token = token.substring(1);
            			}
            		}else {
            			quote_number -= 1;
            			if(end(temp_String.charAt(temp_String.indexOf('"') - 1)))
            			{
            				firstWord = true;
            			}
            			temp_String = "w" + temp_String.substring(temp_String.indexOf('"') + 1);
            		}
            	}
            	
            	if(in_quote)
            	{
            		quote += " ";
            		quote += token;
            	}
            	
            	if (quote_number == 0 && in_quote) {
            		quote = quote.substring(1);
            		while(quote.charAt(quote.length()-1) != '"')
            		{
            			if(end(quote.charAt(quote.length()-1)))
            			{
            				firstWord = true;
            			}
            			quote = quote.substring(0,quote.length()-1);
            		}
            		quote_token = quote.substring(0,quote.length()-1);
            		in_quote = false;
            		quote = "";
            	}
                
            	//////////////////////////////////////////Not quote
            	if(!in_quote && quote_token.equals(""))
            	{
            		
            		if(token.length() >= 10 && token.charAt(4) == '-' && token.charAt(7) == '-')
            		{
            			if(end(token.charAt(token.length()-1)))
            			{
            				firstWord = true;
            				token = removeEnd(token);
            			}
            		}else {
            			if(Character.isUpperCase(token.charAt(0)))
            			{
            				if(name_length == 0)
            				{
            					name_cache = "";
            				}
            				name_cache += " ";
            				name_cache += token;
            				in_name = true;
            				name_length ++;
            				if(name_length > 1)
            				{
            					firstWord = false;
            				}
            				
            				if(end(name_cache.charAt(name_cache.length()-1)))
            				{
            					if(name_length == 1)
            					{
            						name_cache = name_cache.toLowerCase();
            						//name_cache = name_cache.replaceAll("[^a-zA-Z.:|\\/|\\']", "");
            						if(stopAnalyser.isStopWord(name_cache))
                    				{
                    					name_cache = "";
                    				}
                					name_cache = stem(name_cache);
            					}
            					in_name = false;
            					name_length = 0;
            					firstWord = true;
            					name_cache = removeEnd(name_cache);
            					token = name_cache.substring(1);
            				}
            				
            			}else {
            				in_name = false;
            				
            				if(name_length == 1 && firstWord)
            				{
            					name_cache = name_cache.toLowerCase();
            					firstWord = false;
            					
            					if(end(name_cache.charAt(name_cache.length()-1)))
            					{
            						firstWord = true;
            					}
            					name_cache = removeEnd(name_cache);
            					//name_cache = name_cache.replaceAll("[^a-zA-Z.:|\\/|\\']", "");
            					if(stopAnalyser.isStopWord(name_cache))
                				{
            						name_cache = "";
                				}
            					name_cache = stem(name_cache);
            				}
            				if(name_length == 1 && !firstWord && stopAnalyser.isStopWord(name_cache.toLowerCase()))
            				{
            					name_cache = "";
            				}
            				token = token.toLowerCase();
            				if(end(token.charAt(token.length()-1)))
            				{
            					firstWord = true;
            				}
            				token = removeEnd(token);
            				//name_cache = name_cache.replaceAll("[^a-zA-Z.:|\\/|\\']", "");
            				if(stopAnalyser.isStopWord(token))
            				{
            					token = "";
            				}
            				token = stem(token);
            				name_length = 0;
            			}
            		}
            	}
                writer.println(name_cache);
                
                ///////////////////////////////////////////////////Saving tokens in hash map
            	
            	if (in_quote || cache.containsKey(token))
            	{
            		if(cache.containsKey(token))
            		{
            			term_count ++;
            			//cache.put(token, cache.get(token).add(new LongWritable(term_count)));
            			cache.get(token).add(new LongWritable(term_count));
            		}
            		continue;
            	}
            	else {
            		if(cache.containsKey(quote_token))
            		{
            			term_count ++;
            			//cache.put(quote_token, cache.get(quote_token).add(term_count));
            			cache.get(quote_token).add(new LongWritable(term_count));
            		}
            		if(!quote_token.equals("") && !cache.containsKey(quote_token) && !stopAnalyser.isStopWord(quote_token.toLowerCase()))
            		{
            			term_count ++;
            			cache.put(quote_token, new ArrayListWritable());
            			cache.get(quote_token).add(new LongWritable(term_count));
            		}
            		if(quote_token.contentEquals("") && !stopAnalyser.isStopWord(token.toLowerCase()))
            		{
            			term_count ++;
            			//cache.put(token, new ArrayListOfLongs(0).add(term_count));
            			cache.put(token, new ArrayListWritable());
            			cache.get(token).add(new LongWritable(term_count));
            		}
            		quote_token = "";
            	}
            	
            	if (in_name)
            	{
            		continue;
            	}
            	else {
            	
            		if(!name_cache.contentEquals(""))
            		{
            			name_cache = name_cache.substring(1);
            			if(cache.containsKey(name_cache) || stopAnalyser.isStopWord(name_cache.toLowerCase()))
                		{
            				if(cache.containsKey(name_cache))
            				{
            					term_count ++;
                    			//cache.put(name_cache, cache.get(name_cache).add(term_count));
                    			cache.get(name_cache).add(new LongWritable(term_count));
            				}
                			name_cache = "";
                			continue;
                		}
            			term_count ++;
            			//cache.put(name_cache, new ArrayListOfLongs(0).add(term_count));
            			cache.put(name_cache, new ArrayListWritable());
            			cache.get(name_cache).add(new LongWritable(term_count));
            			
            		}
            		name_cache = "";
            	}
            	
            	writer.close();
            }
            
            if (name_cache.length() > 1)
        	{
        		if(cache.containsKey(name_cache.substring(1)))
        		{
        			term_count ++;
        			//cache.put(name_cache.substring(1), cache.get(name_cache.substring(1)).add(term_count));
        			cache.get(name_cache.substring(1)).add(new LongWritable(term_count));
        		}else {
        			term_count ++;
        			//cache.put(name_cache.substring(1), new ArrayListOfLongs(0).add(term_count));
        			cache.put(name_cache.substring(1), new ArrayListWritable());
        			cache.get(name_cache.substring(1)).add(new LongWritable(term_count));
        		}
        	}
            
            
            for(String cache_key : cache.keySet())
    		{	
            	//writer.println(cache_key);
            	TOKEN.set(cache_key);
    			context.write(TOKEN, new PairOfWritables(INPUTFILE,cache.get(cache_key)));
    		}
        }
    }

    public static class Reduce extends Reducer<Text, PairOfWritables<Text,ArrayListWritable<LongWritable>>, Text, ArrayListWritable<PairOfWritables<PairOfWritables<Text,ArrayListWritable<LongWritable>>,DoubleWritable>>>
    {

        // TODO
        // This Reduce Job should take in a key and an iterable of file names
        // It should convert this iterable to a writable array list and output
        // it along with the key
        public void reduce(
                Text key,
                Iterable<PairOfWritables<Text,ArrayListWritable<LongWritable>>> values,
                Context context) throws IOException, InterruptedException
        {
        	//Iterator<Text> iter = values.iterator();
        	//ArrayListWritable<Text> filenames = new ArrayListWritable<Text>();
        	ArrayListWritable<PairOfWritables<PairOfWritables<Text,ArrayListWritable<LongWritable>>,DoubleWritable>> fileinfo = new ArrayListWritable<PairOfWritables<PairOfWritables<Text,ArrayListWritable<LongWritable>>,DoubleWritable>>();
        	
        	for(PairOfWritables<Text,ArrayListWritable<LongWritable>> name : values) {
        		if (!fileinfo.contains(name))
        		{
        			//filenames.add(new Text(name.getLeftElement()));
        			fileinfo.add(new PairOfWritables(new PairOfWritables(name.getLeftElement(), name.getRightElement()),new DoubleWritable(name.getRightElement().size())));
        		}
        	}
        	
        	for(int index = 0; index < fileinfo.size(); index++)
        	{
        		fileinfo.get(index).getRightElement().set((1 + Math.log10(fileinfo.get(index).getRightElement().get())) * (Math.log10(6.0 / fileinfo.size())));
        	}
        	context.write(key, fileinfo);
        }
    }

    // Lets create an object! :)
    public BasicInvertedIndex()
    {
    }

    // Variables to hold cmd line args
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception
    {
        
        // Handle command line args
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline = null;
        CommandLineParser parser = new XParser(true);

        try
        {
            cmdline = parser.parse(options, args);
        }
        catch (ParseException exp)
        {
            System.err.println("Error parsing command line: "
                    + exp.getMessage());
            System.err.println(cmdline);
            return -1;
        }

        // If we are missing the input or output flag, let the user know
        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT))
        {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        // Create a new Map Reduce Job
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
                .parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        // Set the name of the Job and the class it is in
        job.setJobName("Basic Inverted Index");
        job.setJarByClass(BasicInvertedIndex.class);
        job.setNumReduceTasks(reduceTasks);
        
        // Set the Mapper and Reducer class (no need for combiner here)
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        // Set the Output Classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfWritables.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayListWritable.class);

        // Set the input and output file paths
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        // Time the job whilst it is running
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        // Returning 0 lets everyone know the job was successful
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new BasicInvertedIndex(), args);
    }
}
