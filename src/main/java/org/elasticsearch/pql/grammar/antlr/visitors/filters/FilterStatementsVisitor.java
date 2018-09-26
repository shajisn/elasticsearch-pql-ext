package org.elasticsearch.pql.grammar.antlr.visitors.filters;


import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.pql.grammar.antlr.PqlBaseVisitor;
import org.elasticsearch.pql.grammar.antlr.PqlParser.FilterStatementContext;
import org.elasticsearch.pql.grammar.antlr.PqlParser.FilterStatementsContext;

public class FilterStatementsVisitor extends PqlBaseVisitor<QueryBuilder> {

	@Override
    public QueryBuilder visitFilterStatements(FilterStatementsContext ctx) {
		BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
		for(FilterStatementContext statementContext : ctx.filterStatement()) {
			queryBuilder.must(new FilterStatementQueryVisitor().visitFilterStatement(statementContext));
		}
		return queryBuilder;
    }

}
