package org.influxdb.querybuilder.api;

import org.assertj.core.api.Assertions;
import org.influxdb.dto.Query;
import org.influxdb.querybuilder.BuiltQuery;
import org.influxdb.querybuilder.ShowSeriesQueryImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.eq;

class ShowSeriesQueryImplTest {

    @Test
    void testFailureWhenNoDatabaseOrMeasurement() {

        //Given & When
        final String failureMessage = "database and measurement are mandatory fields to query series";
        ShowSeriesQueryImpl query = BuiltQuery.QueryBuilder.series();

        //Then
        assertThatThrownBy(query::getCommand).isInstanceOf(IllegalArgumentException.class)
                .hasMessage(failureMessage);

        //Given && When
        query = BuiltQuery.QueryBuilder.series().from(null, "asdf");

        //Then
        assertThatThrownBy(query::getCommand).isInstanceOf(IllegalArgumentException.class)
                .hasMessage(failureMessage);


        //Given && When
        query = BuiltQuery.QueryBuilder.series().from("database", "  ");

        //Then
        assertThatThrownBy(query::getCommand).isInstanceOf(IllegalArgumentException.class)
                .hasMessage(failureMessage);


    }

    @Test
    void testCreateSimpleSeriesQuery() {

        //Given & When
        Query query = BuiltQuery.QueryBuilder.series().from("database", "measurement");

        //Then
        Assertions.assertThat(query.getCommand()).isEqualTo("SHOW SERIES FROM \"database.measurement\";");
    }

    @Test
    void testCreateSeriesWithCondition() {
        //Given & When
        Query query = BuiltQuery.QueryBuilder.series()
                .from("database", "measurement")
                .where(eq("field1", 3))
                .or(eq("field2", 5));

        //Then
        Assertions.assertThat(query.getCommand())
                .isEqualTo("SHOW SERIES FROM \"database.measurement\" WHERE field1 = 3 OR field2 = 5;");
    }
}