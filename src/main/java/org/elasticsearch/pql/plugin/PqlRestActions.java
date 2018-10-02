package org.elasticsearch.pql.plugin;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.pql.grammar.PqlQuery;
import org.elasticsearch.pql.netty.NettyClient;
import org.elasticsearch.pql.utils.Condition;
import org.elasticsearch.pql.utils.ESUtils;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

import io.netty.channel.ChannelFuture;

public class PqlRestActions extends BaseRestHandler {

	private static final Logger log = LogManager.getLogger(PqlRestActions.class);
	public static final String NAME = "pql";

	private static final int MAX_THREAD = 50;

	private static ExecutorService threadpool = Executors.newFixedThreadPool(MAX_THREAD);

	protected PqlRestActions(Settings settings, RestController controller) {
		super(settings);
		controller.registerHandler(GET, "_pql", this);
		controller.registerHandler(POST, "_pql", this);
		controller.registerHandler(GET, "_pql/{action}", this);
		controller.registerHandler(POST, "_pql/{action}", this);
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

		// ArrayList<Future<ParellelSearchExecutor>> futureObjects = new
		// ArrayList<Future<ParellelSearchExecutor>>();

		String query = restRequest.param("query");
		log.debug("[pql] request for query {} ", query);
		String[] queryParts = query.split("\\|");

		Optional<String> optSearch = Arrays.stream(queryParts)
				.filter(x -> x.indexOf("search") != -1)
				.findFirst();
		if (optSearch.isPresent()) {// Check whether optional has element "search"
			String searchString = optSearch.get().trim();
			String[] searchParts = searchString.split(" ");
			Optional<String> optSearchQuery = Arrays.stream(searchParts)
					.filter(x -> x.indexOf("=") != -1)
					.findFirst();
			// Check if search part exists in input query string
			if (optSearchQuery.isPresent()) {
				String condition = optSearchQuery.get().trim();
				String searchKey = condition.substring(condition.indexOf("=") + 1);
				String searchField = condition.substring(0, condition.indexOf("="));
				log.debug(searchField + " = " + searchKey);

				try {

					String sWords = "";
					NettyClient nettyClient = null;
					try {
						nettyClient = new NettyClient();
						ChannelFuture writeFuture = nettyClient
								.lookup_nlp_data(searchKey.replaceAll("[^a-zA-Z0-9]", ""));
						if (writeFuture != null) {
							writeFuture.sync();
						}
						sWords = nettyClient.getInboundChannel().wait_for_response();
						nettyClient.ShutDown();
					} catch (Exception e) {
						log.error("NLP lookup service failed with error message " + e.getLocalizedMessage());
					}

					List<String> words = null;
					if (sWords.length() > 0) {
						sWords = sWords.replaceAll("^\\[|]$", "");
						words = Arrays.asList(sWords.split(","));
					}
					if (words == null || words.size() == 0) {
						log.warn("NLP search module didn't return any words!!!! Searching with regular PQL commands.");
						String sQuery = ESUtils.stripFilterCommandsFromQuery(query);
						log.info("Orig. Query =" + query + "Stripped query = " + sQuery);
						PqlQuery pqlQuery = new PqlQuery();
						SearchRequestBuilder searchRequestBuilder = pqlQuery.buildRequest(sQuery, client);
						log.info("[pql] request for query {} : request : {}", query, searchRequestBuilder.toString());
						return channel -> searchRequestBuilder
								.execute(new RestBuilderListener<SearchResponse>(channel) {
									@Override
									public RestResponse buildResponse(SearchResponse searchResponse,
											XContentBuilder xContentBuilder) throws Exception {
										return new BytesRestResponse(RestStatus.OK,
												searchResponse.toXContent(xContentBuilder, restRequest));
									}
								});
					} else {
						log.info("NLP search module returned words. Searching in ES for each word. !!! Total words ="
								+ words.size());
						ArrayList<SearchRequestBuilder> searchRequests = new ArrayList<SearchRequestBuilder>();

						words.forEach(word -> {
							log.info("Submitting search request for word " + word);

							List<String> rawQueryFiltered = Arrays.stream(queryParts)
									.filter(x -> x.indexOf("source ") != -1 ) //Add Index query
									.collect(Collectors.toList());
							//Add our new search value returned from NLP service
							rawQueryFiltered.add("search " + searchField + "='" + word.trim() + "'");
							
							List<String> restOfTokens = Arrays.stream(queryParts)
									.filter(x -> (
											x.indexOf("search ") == -1 // Exclude search, filters and source 
											&& x.indexOf("filters ") == -1 
											&& x.indexOf("source ") == -1))
									.collect(Collectors.toList());
							//Add all rest of tokens if any.
							rawQueryFiltered.addAll(restOfTokens);

							String sQuery = rawQueryFiltered.stream().collect(Collectors.joining(" | "));
							log.info("[pql-nlp] Request query : {} ", sQuery);

							PqlQuery pqlQuery = new PqlQuery();
							SearchRequestBuilder searchRequestBuilder = pqlQuery.buildRequest(sQuery, client);
							log.info("[pql-nlp] request for query {} : request : {}", query,
									searchRequestBuilder.toString());

							searchRequests.add(searchRequestBuilder);

						});
						log.info("Executing multi search requet...");
						MultiSearchRequestBuilder multiBuilder = client.prepareMultiSearch();
						searchRequests.forEach(request -> {
							multiBuilder.add(request);
						});
						log.info("Executing Multi serach...");

						return channel -> multiBuilder
								.execute(new RestToXContentListener<MultiSearchResponse>(channel) {
									@Override
									public RestResponse buildResponse(MultiSearchResponse searchResponse,
											XContentBuilder xContentBuilder) throws Exception {
										
										MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[searchResponse.getResponses().length];
										ArrayList<Condition> conditions = new ArrayList<Condition>();
										int i = 0;
										
										Optional<String> lstFilters = Arrays.stream(queryParts)
												.filter(x -> x.indexOf("filters") != -1)
												.findFirst();
										if (lstFilters.isPresent()) {// Check whether optional has element "filters"
											log.info("Applying filters ...");
											String filterString = lstFilters.get().trim();
											
											String[] filterParts = filterString.split(" ");
											List<String> filters = Arrays.stream(filterParts)
													.filter(x -> x.indexOf("filters") == -1)
													.collect(Collectors.toList());
											if (filters != null && filters.size() > 0) {
												log.info("Applying filters ... ");
												
												for (String condition : filters) {
													log.info("Parsing filter condition [{}] "+ condition);
													
													Pattern compile = Pattern.compile("(\\w+)([<>=]+)(\\w+)");
													Matcher matcher = compile.matcher(condition);
													if (matcher.matches()) {
														String leftPart = matcher.group(1);
														String operatorPart = matcher.group(2);
														String rightPart = matcher.group(3);
														conditions.add(new Condition(leftPart.trim(), operatorPart.trim(), rightPart.trim()));
													}
												}
											}
											
										
											for (Iterator<MultiSearchResponse.Item> iterator = searchResponse
													.iterator(); iterator.hasNext();) {
												MultiSearchResponse.Item item = iterator.next();
												SearchResponse response = item.getResponse();
												//Checking if any conditions given...
												if (conditions.size() > 0) {
													boolean shouldAdd = true;
													for (Condition condition : conditions) {
														log.info("Check condition " + condition);
														//Handle min. count condition
														if (condition.getLeftPart().equalsIgnoreCase("count")) {
															int count = 0;
															try {
																count = Integer.parseInt(condition.getRightPart());
															} catch (Exception e) {
																log.info("error parsing minCount "
																		+ e.getLocalizedMessage());
															}
															long hits = response.getHits().getTotalHits();
															log.info("Applying minimum count filter : hits{} count{} " + hits, count);
															if(condition.getOperatorPart().equalsIgnoreCase(">") ) {
																if (hits > count) {
																	log.info("Adding item with hits " + hits);
																	shouldAdd = true; 
																} else {
																	log.info("Filtering item with : hits{} count{} " + hits, count);
																	shouldAdd = false;
																	break;
																}
															} else if(condition.getOperatorPart().equalsIgnoreCase("<") ) {
																if (hits < count) {
																	log.info("Adding item with hits " + hits);
																	shouldAdd = true;
																} else {
																	log.info("Filtering item with : hits{} count{} " + hits, count);
																	shouldAdd = false;
																	break;
																}
															} else if(condition.getOperatorPart().equalsIgnoreCase("=") ) {
																if (hits == count) {
																	log.info("Adding item with hits " + hits);
																	shouldAdd = true;
																} else {
																	log.info("Filtering item with : hits{} count{} " + hits, count);
																	shouldAdd = false;
																	break;
																}
															}
														}
														else {
															log.info("Check cardinals " + condition);
															int minCardinal = 0;
															try {
																minCardinal = Integer.parseInt(condition.getRightPart());
															} catch (Exception e) {
																log.info("error parsing minCount "
																		+ e.getLocalizedMessage());
															}
															Aggregations aggs = response.getAggregations();
															Aggregation agg = aggs.get(condition.getLeftPart());
															if(agg == null) {
																log.info("Check cardinals aggregation element not present. Skip filter");
																continue;
															}
															long cardinalValue = ((Cardinality) (agg)).getValue();
															log.info("Applying cardinal count filter : Cardinals{} count{} " + cardinalValue, minCardinal);
															if(condition.getOperatorPart().equalsIgnoreCase(">") ) {
																if (cardinalValue > minCardinal) {
																	log.info("Adding item with cardinalValue " + cardinalValue);
																	shouldAdd = true;
																} else {
																	log.info("Filtering item with : Cardinals{} cardinalValue{} " + cardinalValue, minCardinal);
																	shouldAdd = false;
																	break;
																}
															} else if(condition.getOperatorPart().equalsIgnoreCase("<") ) {
																if (cardinalValue < minCardinal) {
																	log.info("Adding item with cardinalValue " + cardinalValue);
																	shouldAdd = true;
																} else {
																	log.info("Filtering item with : Cardinals{} cardinalValue{} " + cardinalValue, minCardinal);
																	shouldAdd = false;
																	break;
																}
															} else if(condition.getOperatorPart().equalsIgnoreCase("=") ) {
																if (cardinalValue == minCardinal) {
																	log.info("Adding item with cardinalValue " + cardinalValue);
																	shouldAdd = true;
																} else {
																	log.info("Filtering item with : Cardinals{} cardinalValue{} " + cardinalValue, minCardinal);
																	shouldAdd = false;
																	break;
																}
															}
														}
													}
													
													//If 
													if(shouldAdd) {
														items[i] = new MultiSearchResponse.Item(response, null);
														i++;
													}
													else {
														log.info("Filtering item {} " + response.toString());
													}
												} else {
													items[i] = new MultiSearchResponse.Item(response, null);
													i++;
												}
											}
											
											MultiSearchResponse.Item[] itemsFiltered = new MultiSearchResponse.Item[i];
											for (int j = 0; j < i; j++) {
												itemsFiltered[j] =items[j];
											}
											MultiSearchResponse searchResponseFiltered = new MultiSearchResponse(itemsFiltered);
											
											return new BytesRestResponse(RestStatus.OK,
													searchResponseFiltered.toXContent(xContentBuilder, restRequest));
										}
										else {
											//No filters send all responses
											return new BytesRestResponse(RestStatus.OK,
													searchResponse.toXContent(xContentBuilder, restRequest));
										}
									}
								});


					}
				} catch (Exception e) {
					log.error("Processing failed with message " + e.getLocalizedMessage());
				}
			}
		}
		return createSearchResponse(restRequest, client);
	}

	private RestChannelConsumer createExplainResponse(RestRequest restRequest, NodeClient client) {
		String query = restRequest.param("query");
		PqlQuery pqlQuery = new PqlQuery();
		SearchRequestBuilder searchRequestBuilder = pqlQuery.buildRequest(query, client);
		return restChannel -> restChannel
				.sendResponse(new BytesRestResponse(RestStatus.OK, searchRequestBuilder.toString()));
	}
}
