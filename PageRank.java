//DISHA KARUNAKAR SHETTY
//dshetty1@uncc.edu


import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import training.WordCount.Map;
//import training.WordCount.Reduce;

//import training.WordCount.Reduce;
//import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class PageRank extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( PageRank.class);
	private static final String n = "N";
	public static void main( String[] args) throws  Exception {


		int res  = ToolRunner .run( new PageRank(), args);
		System .exit(res);


	}

	public int run( String[] args) throws  Exception {

		FileSystem fs = FileSystem.get(getConf());

		Job job1  = Job .getInstance(getConf(), " PageRank ");  // Job1 to calculate total number of nodes
		job1.setJarByClass( this .getClass());
		long nvalue;
		FileInputFormat.addInputPaths(job1,  args[0]);
		FileOutputFormat.setOutputPath(job1,  new Path(args[ 1]));
		job1.setMapperClass( Map1 .class);
		job1.setReducerClass( Reduce1.class);
		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( IntWritable .class);
		job1.waitForCompletion( true);

		nvalue = job1.getCounters().findCounter("Result", "Result").getValue();

		Job job  = Job .getInstance(getConf(), " PageRank "); // Job to get set initial page rank to each node along with its links 
		job.setJarByClass( this .getClass());
		FileInputFormat.addInputPaths(job,  args[0]);
		FileOutputFormat.setOutputPath(job,  new Path(args[ 2]+"job0"));
		job.getConfiguration().setStrings(n, nvalue + "");
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( Text .class);
		job.waitForCompletion( true) ;
		int i;
		for( i=1; i<=10;i++)  // iteration to get more accurate page ranks till they converge
		{Job job2  = Job .getInstance(getConf(), " PageRank ");  // Job2 to calculate final page ranks for each node
		job2.setJarByClass( this .getClass());
		FileInputFormat.addInputPaths(job2,  args[2]+"job"+(i-1));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]+"job"+i)); 
		job2.setMapperClass( Map2 .class);
		job2.setReducerClass( Reduce2 .class);
		job2.setOutputKeyClass( Text .class);
		job2.setOutputValueClass( Text .class);

		job2.waitForCompletion( true);
		fs.delete(new Path(args[2]+"job"+(i-1)), true); // to delete files that are generated iteratively
		}


		Job job3  = Job .getInstance(getConf(), " PageRank ");  // Job 3 to sort the nodes in order of ranks
		job3.setJarByClass( this .getClass());

		FileInputFormat.addInputPaths(job3,  args[2]+"job"+(i-1));
		FileOutputFormat.setOutputPath(job3,  new Path(args[ 3]));
		job3.setMapperClass( Map3 .class);
		job3.setReducerClass( Reduce3.class);
		job3.setOutputKeyClass( DoubleWritable .class);
		job3.setOutputValueClass( Text .class);
		job3.waitForCompletion( true);

		return 1;
	}



	//*****JOB1******


	public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable( 1);


		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			// Calculating the value of N using title tags 
			String line  = lineText.toString();    

			Text current_key  = new Text();
			Pattern pattern_key = Pattern .compile("<title>(.*?)</title>");
			Matcher matcher_key = pattern_key.matcher(line);

			while (matcher_key.find()) {

				current_key  = new Text("N");

				context.write(current_key,one);

			} 

		}
	}







	public static class Reduce1 extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
		int sum  = 0;
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
				throws IOException,  InterruptedException {



			for ( IntWritable count  : counts) {
				sum  += count.get();   // Calculating sum  to get N 
			}



			context.write(word,  new IntWritable(sum));
			String sumString=Integer.toString(sum);

		}
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			context.getCounter("Result", "Result").increment(sum);
		}
	}




	//***** JOB2*****
	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		private Text word  = new Text();

		public void configure(JobConf conf)
		{
			System.out.println("abc");
			System.out.println("this is "+conf.get("map.input.file"));
		}

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {


			String line  = lineText.toString();
			if(!line.isEmpty())
			{
				Text current_key  = new Text();
				Text current_value  = new Text();
				Text output2  = new Text();




				Pattern pattern_key = Pattern .compile("<title>(.*?)</title>");  // to get page name
				Matcher matcher_key = pattern_key.matcher(line);
				while (matcher_key.find() )
				{
					current_key  = new Text(matcher_key.group(1));

				}

				Pattern pattern_value = Pattern .compile("\\[\\[(.*?)\\]\\]");  //to get outlinks of the page
				Matcher matcher_value = pattern_value.matcher(line);		
				while ( matcher_value.find()) {

					if(output2.toString().isEmpty())
					{
						output2  = new Text(matcher_value.group(1));

					}
					else
					{
						output2  = new Text(output2+","+matcher_value.group(1)); //appending outlinks for a particular page

					}



					String NoComma=output2.toString();
					current_value  = new Text(NoComma);

				} 

				context.write(current_key,current_value); // writing key-page name and value-list of outlinks


			}

		}

	}

	public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		Text textValue = new Text();
		double N;

		public void setup(Context context) throws IOException, InterruptedException{
			N = context.getConfiguration().getDouble(n, 1); //getting the value of total number of nodes from first job
		}
		@Override 
		public void reduce( Text word,  Iterable<Text > values,  Context context)
				throws IOException,  InterruptedException {
			Text output2  = new Text();



			String sum="";
			int i=0;
			for(Text value: values){

				if(i>0)
				{i=0;
				break;
				}
				else
				{
					String line=(1/N)+"@"+value.toString();  // appending outlinks list after (1/N) separated by delimeter '@'

					output2  = new Text(line);

					context.write(word, output2);

					i++;

				}
				sum=sum+value;


			} 


		}
	}





	//*****JOB 3*****

	public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

	//	private final static IntWritable one  = new IntWritable( 1);
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();
			line=line.trim();

			String pageSplit[]=line.split("\\t"); // to separate page name from rank and outlinks list
			if(pageSplit.length==2)  
			{
				String pageValue=pageSplit[0];  
			//	System.out.println("****"+pageSplit[1]);
				String afterRank[]=pageSplit[1].toString().split("@");   // to separate page rank and list of outlinks
				if(afterRank.length==2)
				{
				{
					float rank_pass=Float.parseFloat(afterRank[0].toString()); 

					String links=afterRank[1]; 

					String seperateLinks[]=links.split(",");  //separating all the outlinks of the page
					int totalNoLinks=seperateLinks.length;  // calculating total outlinks of page 


					// calculating sum of all (PageRank/Count of links) for all outlinks of the page
					for(int i=0;i<seperateLinks.length;i++)
					{
						String linkpage=seperateLinks[i];
						float PC=(rank_pass/totalNoLinks);
						String pByc=String.valueOf(PC);

						Text pageRankTotalLinks=new Text(pByc);

						context.write(new Text(linkpage),pageRankTotalLinks); //writing the outlink of current page as key and (PageRank/Count of links) value

					}
				

					context.write(new Text(pageValue), new Text("@"+links));	// writing page name and outlinks list
				}
			}
			}

		}

	}




	public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  Text > {
		private static final float dampingFactor=0.85F;

		Text key= new Text();
		@Override 
		public void reduce( Text word,  Iterable<Text > values,  Context context)
				throws IOException,  InterruptedException {
			int count=0;


			String links="";

			String reducer2output;
			float PC=0;
			for ( Text value  : values) {


				reducer2output=value.toString();

				if(reducer2output.contains("@"))
				{
					links=reducer2output;


				}
				else
				{

					Float tempPC=Float.parseFloat(reducer2output);
					PC=PC+tempPC;

				}
			}

			float finalPagerank=(1-dampingFactor)+dampingFactor*PC;  // calculating the actual page ranks of page	

			context.write(word, new Text(finalPagerank+links));
		}

	}

	//*****JOB4*****

	public static class Map3 extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
		private final static IntWritable one  = new IntWritable( 1);


		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			// separating the pages name and ranks

			String line  = lineText.toString();

			String getRank[]=line.split("@");
			if(getRank.length==2)
			{
				String temp2=getRank[0].toString();

				String endRank[]=temp2.split("\\t");
				if(endRank.length==2)
				{
					Double	outputPageRank=Double.parseDouble(endRank[1]);
					context.write(new DoubleWritable(-1 * outputPageRank),new Text( endRank[0])); 
				}
			}
		} 

	}


	public static class Reduce3 extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
		int sum  = 0;
		@Override 
		public void reduce( DoubleWritable counts, Iterable<Text> word,
				Context context)
						throws IOException,  InterruptedException {



			double tempValue = 0;		
			tempValue = counts.get() * -1; // to sort in descending
			String name = "";
			for (Text pageName : word) {
				name = pageName.toString();
				context.write(new Text(name), new DoubleWritable(tempValue));

			}

		}
	}
}