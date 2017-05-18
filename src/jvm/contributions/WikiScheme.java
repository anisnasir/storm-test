package contributions;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WikiScheme implements KeyValueScheme {
	private static final long serialVersionUID = 1L;
	@Override
	public Fields getOutputFields() {
		return new Fields("timestamp","word");
	}
	public static String toString(ByteBuffer bb) {
	    final byte[] bytes = new byte[bb.remaining()];
	 
	    bb.duplicate().get(bytes);
	 
	    return new String(bytes);
	}
	@Override
	public List<Object> deserializeKeyAndValue(ByteBuffer k, ByteBuffer v) {
		String  key = toString(k);
		String value = toString(v);
		return new Values(key, value);
	}
	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		// TODO Auto-generated method stub
		return null;
	}
}