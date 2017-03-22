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
		String str = new String( bytes.array(), Charset.forName("ASCII") );
		return new Values(str);
	}
	@Override
	public Fields getOutputFields() {
		return new Fields("str");
	}
}