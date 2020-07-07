package org.influxdb.querybuilder;


import org.influxdb.querybuilder.clauses.AndConjunction;
import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.ConjunctionClause;
import org.influxdb.querybuilder.clauses.OrConjunction;

import java.util.ArrayList;
import java.util.List;

public class ShowSeriesQueryImpl extends BuiltQuery {

    private final String measurement;

    ShowSeriesQueryImpl() {
        super(null);
        this.measurement = null;
    }

    ShowSeriesQueryImpl(final String database, final String measurement) {
        super(database);
        this.measurement = measurement;
    }

    public ShowSeriesQueryImpl from(final String database, final String measurement) {
        return new ShowSeriesQueryImpl(database, measurement);
    }

    public ShowSeriesWhereQueryImpl where() {
        return where(null);
    }

    public ShowSeriesWhereQueryImpl where(final Clause clause) {
        ShowSeriesWhereQueryImpl showSeriesWhereQuery = new ShowSeriesWhereQueryImpl(this);
        if (clause != null) {
            showSeriesWhereQuery.and(clause);
        }
        return showSeriesWhereQuery;
    }

    @Override
    public StringBuilder buildQueryString(final StringBuilder builder) {

        if (getDatabase() == null || getDatabase().trim().isEmpty()
                || measurement == null || measurement.trim().isEmpty()) {

            throw new IllegalArgumentException("database and measurement are mandatory fields to query series");
        }

        builder.append("SHOW SERIES FROM ")
                .append("\"")
                .append(String.join(".", getDatabase(), measurement))
                .append("\"");

        return builder;
    }

    @Override
    public StringBuilder buildQueryString() {
        return null;
    }

    public static final class ShowSeriesWhereQueryImpl extends BuiltQuery implements Where {

        private List<ConjunctionClause> clauses = new ArrayList<>();

        private final ShowSeriesQueryImpl showSeriesQuery;

        private ShowSeriesWhereQueryImpl(final ShowSeriesQueryImpl showSeriesQuery) {
            super(showSeriesQuery.getDatabase());
            this.showSeriesQuery = showSeriesQuery;
        }

        @Override
        public ShowSeriesWhereQueryImpl and(final Clause clause) {
            clauses.add(new AndConjunction(clause));
            return this;
        }

        @Override
        public ShowSeriesWhereQueryImpl or(final Clause clause) {
            clauses.add(new OrConjunction(clause));
            return this;
        }

        @Override
        public List<ConjunctionClause> getClauses() {
            return clauses;
        }

        @Override
        public WhereNested andNested() {
            throw new RuntimeException("Not implemented yet");
        }

        @Override
        public WhereNested orNested() {
            throw new RuntimeException("Not implemented yet");
        }

        @Override
        public <T extends Select> T orderBy(final Ordering orderings) {
            return null;
        }

        @Override
        public <T extends Select> T groupBy(final Object... columns) {
            return null;
        }

        @Override
        public <T extends Select> T limit(final int limit) {
            return null;
        }

        @Override
        public <T extends Select> T limit(final int limit, final long offSet) {
            return null;
        }


        @Override
        public StringBuilder buildQueryString(final StringBuilder builder) {

            showSeriesQuery.buildQueryString(builder);

            if (!this.getClauses().isEmpty()) {
                builder.append(" WHERE ");
                Appender.joinAndAppend(builder, clauses);
            }

            return builder;
        }

        @Override
        public StringBuilder buildQueryString() {
            return buildQueryString(new StringBuilder());
        }
    }
}
