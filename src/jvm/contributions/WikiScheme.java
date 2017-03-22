package contributions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

public class WikiScheme implements Scheme{

	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		// TODO Auto-generated method stub
		ByteArrayInputStream in = new ByteArrayInputStream(ser.array());
		List<Object> results = new ArrayList<Object>();
		try {
			ObjectInputStream is = new ObjectInputStream(in);
			results.add(is.readObject());
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return new Fields("timestamp", "word");
	}

}
