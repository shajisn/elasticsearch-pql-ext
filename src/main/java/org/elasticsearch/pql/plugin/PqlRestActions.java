package org.elasticsearch.pql.plugin;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.pql.grammar.PqlQuery;
import org.elasticsearch.pql.nlp.NLPSequenceSearcher;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

public class PqlRestActions extends BaseRestHandler {

	private static final Logger log = LogManager.getLogger(PqlRestActions.class);
	public static final String NAME = "pql";
	
	private static final int MAX_THREAD = 50;
	
	private static ExecutorService threadpool = Executors.newFixedThreadPool(MAX_THREAD);
	
	private NLPSequenceSearcher nlpSearchInstance = null;

	protected PqlRestActions(Settings settings, RestController controller) {
		super(settings);
		controller.registerHandler(GET, "_pql", this);
		controller.registerHandler(POST, "_pql", this);
		controller.registerHandler(GET, "_pql/{action}", this);
		controller.registerHandler(POST, "_pql/{action}", this);
		
		try {
			nlpSearchInstance = NLPSequenceSearcher.getInstance();
		} catch (Exception e) {
			log.error("Loading NLP search module failed with error : " + e.getMessage());
		}
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
		log.debug("Handle [pql] endpoint request :", request);
		if ("explain".equalsIgnoreCase(request.param("action"))) {
			return createExplainResponse(request, client);
		} else {
			return createNLPSearchResponse(request, client);
		}
	}

	private RestChannelConsumer createSearchResponse(RestRequest restRequest, NodeClient client) {
		String query = restRequest.param("query");
		PqlQuery pqlQuery = new PqlQuery();
		SearchRequestBuilder searchRequestBuilder = pqlQuery.buildRequest(query, client);
		logger.debug("[pql] request for query {} : request : {}", query, searchRequestBuilder.toString());

		return channel -> searchRequestBuilder.execute(new RestBuilderListener<SearchResponse>(channel) {
			@Override
			public RestResponse buildResponse(SearchResponse searchResponse, XContentBuilder xContentBuilder)
					throws Exception {
				return new BytesRestResponse(RestStatus.OK, searchResponse.toXContent(xContentBuilder, restRequest));
			}
		});
	}

	private RestChannelConsumer createNLPSearchResponse(RestRequest restRequest, NodeClient client) {
		
		if(nlpSearchInstance == null) {
			log.warn("NLP search module is not loaded. Searching with regular PQL commands.");
			return createSearchResponse(restRequest, client);
		}
		
		ArrayList<Future<ParellelSearchExecutor>> futureObjects = new ArrayList<Future<ParellelSearchExecutor>>();
		
		String query = restRequest.param("query");
		logger.debug("[pql] request for query {} ", query);
		String []queryParts = query.split("\\|"); 
		
		Optional<String> optSearch = Arrays.stream(queryParts)
                .filter(x -> x.indexOf("search") != -1)
                .findFirst();
		if(optSearch.isPresent()) {//Check whether optional has element "search"
			String searchString = optSearch.get().trim();
			String []searchParts = searchString.split(" ");
			Optional<String> optSearchQuery = Arrays.stream(searchParts)
	                .filter(x -> x.indexOf("=") != -1)
	                .findFirst();
			//Check if search part exists in input query string
			if(optSearchQuery.isPresent()) {
				String condition = optSearchQuery.get().trim();
				String searchKey = condition.substring(condition.indexOf("=") + 1);
				String searchField = condition.substring(0, condition.indexOf("="));
				logger.debug(searchField + " = " + searchKey);
				
				try {
					Collection<String> words = nlpSearchInstance.findNearWords(searchKey);
					words.forEach(word->{
						log.info("Submitting search request for word " + word);
						ParellelSearchExecutor obj = new ParellelSearchExecutor(restRequest, client, word, searchField);
						Future<ParellelSearchExecutor> future = threadpool.submit(obj);
						futureObjects.add(future);
					});
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		ArrayList<SearchResponse> searchResponses = new ArrayList<SearchResponse>(); 
		//Wait till all searches are done
		while (!futureObjects.isEmpty()) {
			for (Iterator<Future<ParellelSearchExecutor>> iterator = futureObjects.iterator(); iterator.hasNext();) {
				try {
					Future<ParellelSearchExecutor> future = iterator.next();
					ParellelSearchExecutor obj = future.get();
					String word = obj.getSearchWord();
					if (future.isDone()) {
						log.debug("Search is completed yet word [" + word + "]... Hits = " + obj.getSearchResponse().getHits());
						
						iterator.remove();
					} else
						log.debug("Search is not completed yet for table [" + word + "]...");

				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			try {
				//Wait for 10 ms before next poll to check search completion.
				if(!futureObjects.isEmpty())
					Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//All searches using keywords completed. Join the results and send.
		
		return null;
	}

	private RestChannelConsumer createExplainResponse(RestRequest restRequest, NodeClient client) {
		String query = restRequest.param("query");
		PqlQuery pqlQuery = new PqlQuery();
		SearchRequestBuilder searchRequestBuilder = pqlQuery.buildRequest(query, client);
		return restChannel -> restChannel
				.sendResponse(new BytesRestResponse(RestStatus.OK, searchRequestBuilder.toString()));
	}
}
