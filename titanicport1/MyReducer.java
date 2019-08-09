package titanicport1;

import java.io.IOException;

//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException {
		int sum = 0;	
		int i=0,j=0,k=0;
		Text poepluspclass1=null;
		Text poepluspclass2=null;
		Text poepluspclass3=null;
		String poe = null;
		for (Text value : values)
		{
			String line=value.toString();
			String lineparts[]=line.split(",");
			String pclass=lineparts[1];
			poe= lineparts[11];
			if(pclass.equals("1"))
			{
				i++;
			}
			if(pclass.equals("2"))
			{
				j++;
			}
			if(pclass.equals("3"))
			{
				k++;
			}
			sum++;
			
		}
		double percentage1=((double)i/sum)*100;
		double percentage2=((double)j/sum)*100;
		double percentage3=((double)k/sum)*100;
		Text p1=new Text("percentage of female those are first class passenger in port:"+percentage1);
		Text p2=new Text("percentage of female those are second class passenger in port:"+percentage2);
		Text p3=new Text("percentage of female those are third class passenger in port:"+percentage3);
		
		poepluspclass1= new Text("Port of Embarkation:"+poe+"  pclass of female passenger:1st");
		poepluspclass2= new Text("Port of Embarkation:"+poe+"  pclass of female passenger:2nd");
		poepluspclass3= new Text("Port of Embarkation:"+poe+"  pclass of female passenger:3rd");
		context.write(poepluspclass1,p1); 
		context.write(poepluspclass2,p2);
		context.write(poepluspclass3,p3);
		
	}
}
                  
