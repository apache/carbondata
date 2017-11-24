package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;

public class FilterExpressParser extends QueryParser {
    private int conj = CONJ_NONE;
    private int mods = MOD_NONE;
    static final int CONJ_NONE = 0;
    static final int CONJ_AND = 1;
    static final int CONJ_OR = 2;
    static final int MOD_NONE = 0;
    static final int MOD_NOT = 10;
    static final int MOD_REQ = 11;

    public FilterExpressParser(String field, Analyzer analyzer) {
        super(field == null ? new String() : field, analyzer);
    }

    private void walkFilterExpr(List <BooleanClause> clauses, Expression expression) throws ParseException {
        Query q = null;
        if (expression == null) {
            return;
        }

        ExpressionType type = expression.getFilterExpressionType();
        switch (type) {
            case AND:
                conj = CONJ_AND;
                return;

            case OR:
                conj = CONJ_OR;
                return;

            case NOT:
                mods = MOD_NOT;
                return;

            case EQUALS:
                break;

            case NOT_EQUALS:
                mods = MOD_NOT;
                break;

            case LESSTHAN:
            case LESSTHAN_EQUALTO:
                break;

            case GREATERTHAN:
                break;
            case GREATERTHAN_EQUALTO:
                break;

            case ADD:
            case SUBSTRACT:
            case DIVIDE:
            case MULTIPLY:
                break;

            case IN:
                break;

            case LIST:
                break;

            case NOT_IN:
                break;

            case UNKNOWN:
                break;

            case LITERAL:
            case RANGE:
                break;

            case FALSE:
            case TRUE:
                break;

            default:
                throw new ParseException("unknow operator type = "  + type);
        }


        addClause(clauses, conj, mods, q);
    }

    public Query parserFilterExpr(FilterResolverIntf filterExp) throws ParseException {
        List <BooleanClause> clauses = new ArrayList <BooleanClause>();

        walkFilterExpr(clauses, filterExp.getFilterExpression());

        if (clauses.size() == 1) {
            return clauses.get(0).getQuery();
        } else {
            return getBooleanQuery(clauses);
        }
    }

    public static Query transformFilterExpress(FilterResolverIntf filterExp) {
        FilterExpressParser parser = new FilterExpressParser(null, new StandardAnalyzer());
        Query query = null;
        try {
            query = parser.parserFilterExpr(filterExp);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return query;
    }
}