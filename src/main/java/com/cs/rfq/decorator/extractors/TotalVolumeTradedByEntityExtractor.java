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

public class TotalVolumeTradedByEntityExtractor implements RfqMetadataExtractor {

    private String lastWeek;
    private String lastMonth;
    private String lastYear;

    public TotalVolumeTradedByEntityExtractor() {
        long pastWeekMs = DateTime.now().minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.now().minusMonths(1).getMillis();
        long pastYearMs = DateTime.now().minusYears(1).getMillis();
        DateFormat dtFormat=new SimpleDateFormat("yyyy-MM-dd");
        this.lastWeek=dtFormat.format(new Date(pastWeekMs));
        this.lastMonth=dtFormat.format(new Date(pastMonthMs));
        this.lastYear=dtFormat.format(new Date(pastYearMs));
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        Object volumeWeek=getVolume(rfq,session,trades,this.lastWeek);
        Object volumeMonth=getVolume(rfq,session,trades,this.lastMonth);
        Object volumeYear=getVolume(rfq,session,trades,this.lastYear);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(RfqMetadataFieldNames.volumeTradedByEntityPastWeek, volumeWeek);
        results.put(RfqMetadataFieldNames.volumeTradedByEntityPastMonth, volumeMonth);
        results.put(RfqMetadataFieldNames.volumeTradedByEntityPastYear, volumeYear);
        return results;
    }

    Object getVolume(Rfq rfq,SparkSession session,Dataset<Row> trades ,String since){
        String query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }
        return volume;
    }

    public void setLastWeek(String lastWeek) {
        this.lastWeek = lastWeek;
    }

    public void setLastMonth(String lastMonth) {
        this.lastMonth = lastMonth;
    }

    public void setLastYear(String lastYear) {
        this.lastYear = lastYear;
    }
}
