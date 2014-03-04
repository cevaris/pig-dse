
package com.dse.pig.udfs;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.dse.cassandra.UUIDType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UUID extends EvalFunc<String> {
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    public UUID() {
        super();
    }

    public String exec(Tuple input) throws IOException {
    	if (input == null || input.size() == 0)
    		return null;
		
    	try{
    		DataByteArray bline = (DataByteArray)input.get(0);
    		String line = bline.toString();
			if (line.length() > 0) {
				Map<String, Object> values = new HashMap<String, Object>();
				System.out.println("Got string " + line);
//				org.apache.cassandra.db.marshal.UUIDType.instance.compose(new ByteBuffer );// uuid = UUIDType.fromString(line);
				return line;
//				values.put("uuid", UUIDType.fromString(line));
//				Tuple t = tupleFactory.newTuple(values);
//	            if (t != null) {
//	                return t;
//	            }
	        }
	        return null;
		}catch(Exception e){
			throw new IOException(e);
		}
    	
    }
    
}