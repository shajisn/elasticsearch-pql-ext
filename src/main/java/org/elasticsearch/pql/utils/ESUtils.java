package org.elasticsearch.pql.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ESUtils {
	
	public static String removeNonAscii(String str)
	{
	    return str.replaceAll("[^\\x00-\\x7F]", "");
	}
	
	public static String stripFilterCommandsFromQuery(String query) {
		String[] queryParts = query.split("\\|");

		List<String> rawQueryFiltered = Arrays.stream(queryParts)
				.filter(x -> x.indexOf("filters") != -1)
				.collect(Collectors.toList());

		String sQuery = rawQueryFiltered.stream()
				.collect(Collectors.joining(" | "));
		return sQuery;

	}
}
