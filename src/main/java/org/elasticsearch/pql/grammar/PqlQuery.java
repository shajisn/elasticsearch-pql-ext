package org.elasticsearch.pql.grammar;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.pql.grammar.antlr.PqlAntlrErrorListener;
import org.elasticsearch.pql.grammar.antlr.PqlAntlrErrorStrategy;
import org.elasticsearch.pql.grammar.antlr.PqlLexer;
import org.elasticsearch.pql.grammar.antlr.PqlParser;
import org.elasticsearch.pql.grammar.antlr.visitors.SectionsVisitor;
import org.elasticsearch.pql.grammar.antlr.visitors.search.SearchStatementVisitors;
import org.elasticsearch.pql.plugin.SearchRequestBuilderExt;

public class PqlQuery {

    public QueryBuilder buildQuery(String query) {
        try {
            return SearchStatementVisitors.searchStatementsToQuery(buildAntlrParser(query).searchStatements());
        } catch (PqlException e) {
            throw e;
        } catch (Exception e) {
            throw new PqlException(e.getMessage(), e);
        }
    }

    public SearchRequestBuilder buildRequest(String query, NodeClient client) {
       /* ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(new PlqlBaseListener(), tree);*/
        try {
            return SectionsVisitor.sections(client, buildAntlrParser(query).sections());
        } catch (PqlException e) {
            throw e;
        } catch (Exception e) {
            throw new PqlException(e.getMessage(), e);
        }
    }
    
    private PqlParser buildAntlrParser(String query) {
        ANTLRInputStream antlrInputStream = new ANTLRInputStream(query);
        PqlLexer lexer = new PqlLexer(antlrInputStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PqlParser parser = new PqlParser(tokens);
        // parser.removeErrorListeners();
        parser.addErrorListener(new PqlAntlrErrorListener());
        parser.setErrorHandler(new PqlAntlrErrorStrategy());
        return parser;
    }
    
    public static void main(String[] args) throws Exception {
    	System.out.println("Testing PqlQuery ....");
    	String obj = "source 'nlp_corpus' |  search  description='chance*' | filters mincount=10 | filters author=5";
    	
//    	String []queryParts = obj.split("\\|"); 
//		
//		Optional<String> optSearch = Arrays.stream(queryParts)
//                .filter(x -> x.indexOf("search") != -1)
//                .findFirst();
//		if(optSearch.isPresent()) {//Check whether optional has element "search"
//			String searchString = optSearch.get().trim();
//			String []searchParts = searchString.split(" ");
//			Optional<String> optSearchQuery = Arrays.stream(searchParts)
//	                .filter(x -> x.indexOf("=") != -1)
//	                .findFirst();
//			if(optSearchQuery.isPresent()) {
//				String condition = optSearchQuery.get().trim();
//				String searchKey = condition.substring(condition.indexOf("=") + 1);
//				String searchField = condition.substring(0, condition.indexOf("="));
//				System.out.println(searchKey + " = " + searchField);
//			}
//		}
    	
    	String []queryParts = obj.split("\\|");  
		
		List rawQueryFiltered = Arrays.stream(queryParts)
			  .filter(x -> x.indexOf("search") == -1)
			  .filter(x -> x.indexOf("filters") == -1)
			  .collect(Collectors.toList());
		
		rawQueryFiltered.add("search " + "test" + "=" + "values");
		
		System.out.println("ret = " + rawQueryFiltered);
    	
//    	Settings settings = Settings.builder()
//    			.put("path.home", ".")
//    			.put("transport.type", "local")
//    			.put("discovery.zen.ping.unicast.hosts","192.168.20.219")
//    			.build();
//    	
//    	Node node = new Node(settings);

//       	PqlQuery pqlQuery = new PqlQuery();
//    	PqlParser pqlParser = pqlQuery.buildAntlrParser("source 'nlp_corpus' |  search  description='chance*' | filters mincount=10 | filters author=5");
//    	SectionsContext ctxSections = pqlParser.sections();
//    	for (FilterStatementContext filterStatement : ctxSections.filterStatement()) {
//    		System.out.println("Text= " + filterStatement.getText()); 
//    		System.out.println("Exp= " + filterStatement.booleanExpression().getText()); 
    		
//    		QueryBuilder qb = new FilterStatementQueryVisitor().visitFilterStatement(filterStatement);
//    		System.out.println("QueryName: " + qb.queryName());
//    		System.out.println("Name: " + qb.getName());
//    		System.out.println("Query: " + qb);
//        }
//    	new SectionsVisitor(null).visitSections(ctxSections);
//    	System.out.println(ctxSections.getText());
//    	node.close();
    }
}
