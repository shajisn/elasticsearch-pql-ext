package org.elasticsearch.pql.plugin;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.pql.grammar.PqlQuery;
import org.elasticsearch.rest.RestRequest;

public class ParellelSearchExecutor implements Callable<ParellelSearchExecutor>{
	
	private static final Logger log = LogManager.getLogger(PqlRestActions.class);
	
	private String word = null;
	private String field = null;
	

	private RestRequest restRequest;
	private NodeClient client;
	
	private SearchResponse searchResponse;
	
	public ParellelSearchExecutor(RestRequest restRequest, NodeClient client, String word, String field) {
		this.setSearchWord(word);
		this.field = field;
		this.restRequest = restRequest;
		this.client = client;
	}

	@Override
	public ParellelSearchExecutor call() throws Exception {
		
		String query = restRequest.param("query");
		String []queryParts = query.split("\\|"); 
		
		List<String> rawQueryFiltered = Arrays.stream(queryParts)
			  .filter(x -> x.indexOf("search") == -1)
			  .filter(x -> x.indexOf("filters") == -1)
			  .collect(Collectors.toList());
		
		rawQueryFiltered.add("search " + this.field + "=" + this.word);
		
		String sQuery = rawQueryFiltered.stream()
				  .collect(Collectors.joining(" | "));
		
		PqlQuery pqlQuery = new PqlQuery();
		SearchRequestBuilder searchRequestBuilder = pqlQuery.buildRequest(sQuery, client);
		log.debug("[pql] request for query {} : request : {}", query, searchRequestBuilder.toString());
		
		setSearchResponse(searchRequestBuilder.execute().get());
		return this;
	}

	public SearchResponse getSearchResponse() {
		return searchResponse;
	}

	public void setSearchResponse(SearchResponse searchResponse) {
		this.searchResponse = searchResponse;
	}

	public String getSearchWord() {
		return word;
	}

	public void setSearchWord(String word) {
		this.word = word;
	}
	
	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}
}
