package contributions;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WikiScheme implements Scheme {
	private static final long serialVersionUID = 1L;
	@Override
	public List<Object> deserialize(ByteBuffer bytes) {
		//long  key = bytes.getLong();
		String value = "";
				
		if(bytes.hasArray())
			value = bytes.toString(); 
		return new Values(value);
	}
	@Override
	public Fields getOutputFields() {
		return new Fields("word");
	}
	public static String toString(ByteBuffer bb) {
	    final byte[] bytes = new byte[bb.remaining()];
	 
	    bb.duplicate().get(bytes);
	 
	    return new String(bytes);
	}
}