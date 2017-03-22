package contributions;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.storm.kafka.StringScheme;
import org.junit.Test;

public class WikiSchemeTest {

	@Test
	public void testDeserializeString() {
		String s = "10000	foo";
		byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
		ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
		direct.put(bytes);
		direct.flip();
		String s1 = StringScheme.deserializeString(ByteBuffer.wrap(bytes));
		String s2 = StringScheme.deserializeString(direct);
		assertEquals(s, s1);
		assertEquals(s, s2);
	}

}
