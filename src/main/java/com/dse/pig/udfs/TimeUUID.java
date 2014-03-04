
package com.dse.pig.udfs;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TimeUUID extends EvalFunc<Tuple> {
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    public TimeUUID() {
        super();
    }

    public Tuple exec(Tuple input) throws IOException {
    	if (input == null || input.size() == 0)
    		return null;
		
    	try{
			String line = (String)input.get(0);
			if (line.length() > 0) {
				Map<String, Object> values = new HashMap<String, Object>();
				Tuple t = tupleFactory.newTuple(values);
	            if (t != null) {
	                return t;
	            }
	        }
	        return null;
		}catch(Exception e){
			throw new IOException(e);
		}
    	
    }
    
}