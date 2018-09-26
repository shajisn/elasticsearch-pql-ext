package org.elasticsearch.pql.grammar.antlr.visitors.filters;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.pql.grammar.antlr.PqlBaseVisitor;
import org.elasticsearch.pql.grammar.antlr.PqlParser.FilterStatementContext;
import org.elasticsearch.pql.grammar.antlr.visitors.expression.BooleanExpressionVisitors;

public class FilterStatementQueryVisitor extends PqlBaseVisitor<QueryBuilder> {

	@Override
	public QueryBuilder visitFilterStatement(FilterStatementContext ctx) {
		return BooleanExpressionVisitors.booleanExpressionToQuery(ctx.booleanExpression());
	}
	
	public String getFilterStatement(FilterStatementContext ctx) {
		return ctx.booleanExpression().getText();
	}

}
