package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuySellRatioExtractor implements RfqMetadataExtractor{

    private DateTime current;

    public BuySellRatioExtractor () {
        current = new DateTime();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        ArrayList<String> since = new ArrayList<>(2);
        since.add(current.minusWeeks(1).toString());
        since.add(current.minusMonths(1).toString());

        ArrayList<Object> ratios = new ArrayList<>(2);

        for(String period: since) {
            String query = String.format("SELECT Side, sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' GROUP BY Side ORDER BY Side ASC",
                    rfq.getEntityId(),
                    rfq.getIsin(),
                    period);

            trades.createOrReplaceTempView("trade");
            Dataset<Row> sqlQueryResults = session.sql(query);

            List<Row> rows = sqlQueryResults.collectAsList();

            if(rows.size() == 0) {
                ratios.add("-1");
            }
            else if (rows.size() == 1) {
                if(rows.get(0).get(0).toString().equals("1")) {
                    ratios.add("Infinity");
                }
                else{
                    ratios.add("0");
                }
            }
            else{
                Long buyVolume = (Long) rows.get(0).get(1);
                Long sellVolume = (Long) rows.get(1).get(1);

                ratios.add( Double.toString( buyVolume.doubleValue() / sellVolume.doubleValue()));
            }
//            ratios.add(sqlQueryResults.first().get(0)==null ? 0L : sqlQueryResults.first().get(0));
//            ratios.add(sqlQueryResults.().get(0)==null ? 0L : sqlQueryResults.first().get(0));
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.buySellRatioPastWeek, ratios.get(0));
        results.put(RfqMetadataFieldNames.buySellRatioPastMonth, ratios.get(1));
        return results;
    }

    protected void setCurrent(DateTime current) {
        this.current = current;
    }

}
