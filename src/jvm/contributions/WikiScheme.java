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
		return new Values(new String( bytes.array(), Charset.forName("UTF-8") ));
	}
	@Override
	public Fields getOutputFields() {
		return new Fields("str");
	}
}