package org.elasticsearch.pql.plugin;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class SearchRequestBuilderExt extends SearchRequestBuilder {

	private List<String> lstFilters = new ArrayList<String>(); 
	
	public SearchRequestBuilderExt(ElasticsearchClient client, SearchAction action) {
		super(client, action);
	}

	public List<String> getFilters() {
		return lstFilters;
	}

	public void setFilters(List<String> lstFilters) {
		this.lstFilters = lstFilters;
	}
}
