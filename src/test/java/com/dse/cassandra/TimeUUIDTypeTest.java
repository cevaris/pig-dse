package com.dse.cassandra;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class TimeUUIDTypeTest {

	@Test
	public void test() {
//		String source = "8b685e40-0cd4-4516-bacd-b005dd94f569";
		String source = "f921ca30-a03c-11e3-bdc4-9f708f844dd4";
		String expected = "?O]??4??p??M?";
		ByteBuffer buffer = TimeUUIDType.instance.fromString(source);
		System.out.println(buffer.asCharBuffer());
		System.out.println(expected);
	}

}
