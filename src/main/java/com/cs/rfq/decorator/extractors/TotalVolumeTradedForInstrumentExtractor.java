package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TotalVolumeTradedForInstrumentExtractor implements RfqMetadataExtractor {

    private String since;

    public TotalVolumeTradedForInstrumentExtractor() {
        long pastMonthMs = DateTime.now().minusMonths(1).getMillis();
        DateFormat dtformat=new SimpleDateFormat("yyyy-MM-dd");
        this.since=dtformat.format(new Date(pastMonthMs));

    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String query = String.format("SELECT sum(LastQty) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedForInstrumentPastMonth, volume);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
