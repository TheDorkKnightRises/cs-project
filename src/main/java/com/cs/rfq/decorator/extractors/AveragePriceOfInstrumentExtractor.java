package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class AveragePriceOfInstrumentExtractor implements RfqMetadataExtractor {

    private String since;

    public AveragePriceOfInstrumentExtractor () {
        this.since = new DateTime().minusWeeks(1).toString();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String query = String.format("SELECT avg(LastPx) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> queryResult = session.sql(query);

        Object averagePrice = queryResult.first().get(0);

        if (averagePrice == null) {
            averagePrice = 0;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.averageInstrumentPrice, averagePrice);
        return results;
    }

    protected void setSince (String since) {
        this.since = since;
    }
}
