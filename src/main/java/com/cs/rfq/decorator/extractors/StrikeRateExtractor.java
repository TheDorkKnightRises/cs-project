package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.RfqDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class StrikeRateExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String filePath = "src\\test\\resources\\trades\\rfq.json";
        Dataset<Row> rfqs = new RfqDataLoader().loadRfq(session, filePath);
        System.out.println("trades 1 count"+trades.count());
        return this.extractMetaData(rfq, session, trades, rfqs);
    }

    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> rfqs) {
        System.out.println("rfq count"+rfqs.count());
        System.out.println("trades count"+trades.count());
        System.out.println("trader id"+rfq.getTraderId());
        String query1 = String.format("SELECT rfq.id from trade INNER JOIN rfq ON trade.OrderID = rfq.id where trade.TraderId = %s",
                String.valueOf(rfq.getTraderId()));
        String query2 = String.format("SELECT id from rfq where traderId = %s",
                String.valueOf(rfq.getTraderId()));


        trades.createOrReplaceTempView("trade");
        rfqs.createOrReplaceTempView("rfq");
        Dataset<Row> sqlQueryResults1 = session.sql(query1);
        Dataset<Row> sqlQueryResults2 = session.sql(query2);
        System.out.println("sql reult query1 "+sqlQueryResults1.count());
        System.out.println("sql reult query2 "+sqlQueryResults2.count());


        Object rfq_resulted_trades = sqlQueryResults1.count();
        if (rfq_resulted_trades == null) {
            rfq_resulted_trades = 0L;
        }

        Object rfq_by_trader = sqlQueryResults2.count();
        if (rfq_by_trader == null) {
            rfq_by_trader = 0L;
        }
        System.out.println(rfq_by_trader);
        System.out.println(rfq_resulted_trades);
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.strikeRate,
                (Float.parseFloat(rfq_resulted_trades.toString()) / Float.parseFloat(rfq_by_trader.toString())) * 100);
        return results;
    }

}
