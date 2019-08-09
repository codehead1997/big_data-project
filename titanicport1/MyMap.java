package titanicport1;


import java.io.IOException;

//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text,Text,Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String Txn = value.toString();
			String TxnParts[] = Txn.split(",");
			String PortofEmbarkation=TxnParts[11];
			Text poe = null;
			if(PortofEmbarkation.equals("S"))
			{
				poe=new Text("Southampton");
			}
			if(PortofEmbarkation.equals("Q"))
			{
				poe=new Text("Queenstown");
			}
			if(PortofEmbarkation.equals("C"))
			{
				poe=new Text("Cherbourg");
			}
			
			//Text Tier=new Text(Pclass);
			String sex=TxnParts[4];
			if(key.get()!=0)
			{
				if(sex.equals("female"))
					context.write(poe,value);
			}
	}
}