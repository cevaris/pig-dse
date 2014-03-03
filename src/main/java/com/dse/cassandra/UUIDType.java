package com.dse.cassandra;

import java.util.UUID;

public class UUIDType {

	public static UUID fromString(String source) {
//		String uuidString = "28442f00-2e98-11e2-0000-89a3a6fab5ef";
		UUID uuid = UUID.fromString(source);
		return uuid;
	}

}
