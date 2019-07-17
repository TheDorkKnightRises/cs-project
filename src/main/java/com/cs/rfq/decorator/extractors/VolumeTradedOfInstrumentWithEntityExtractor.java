package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class VolumeTradedOfInstrumentWithEntityExtractor implements RfqMetadataExtractor{

    private DateTime current;

    public VolumeTradedOfInstrumentWithEntityExtractor () {
        current = new DateTime();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        ArrayList<String> since = new ArrayList<>(3);
        since.add(current.minusWeeks(1).toString());
        since.add(current.minusMonths(1).toString());
        since.add(current.minusYears(1).toString());

        ArrayList<Object> volumes = new ArrayList<>(3);

        for(String period: since) {
            String query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                    rfq.getEntityId(),
                    rfq.getIsin(),
                    period);

            trades.createOrReplaceTempView("trade");
            Dataset<Row> sqlQueryResults = session.sql(query);

            volumes.add(sqlQueryResults.first().get(0)==null ? 0L : sqlQueryResults.first().get(0));
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastWeek, volumes.get(0));
        results.put(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastMonth, volumes.get(1));
        results.put(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastYear, volumes.get(2));
        return results;
    }

    protected void setCurrent(DateTime current) {
        this.current = current;
    }
}
