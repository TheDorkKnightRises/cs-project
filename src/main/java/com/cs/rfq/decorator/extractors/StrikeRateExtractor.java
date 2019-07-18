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

        String filePath = "C:\\Users\\Associate\\Desktop\\CS_Project\\cs-project\\src\\test\\resources\\com\\cs\\rfq\\decorator\\rfq.json";
        Dataset<Row> rfqs = new RfqDataLoader().loadRfq(session, filePath);
        //SELECT column_name(s)
        //FROM table1
        //INNER JOIN table2
        //ON table1.column_name = table2.column_name;
        String query1 = String.format("SELECT count(rfq.id) from trade INNER JOIN rfq ON trade.OrderID = rfq.id where trade.TraderId=%s",
                String.valueOf(rfq.getTraderId()));
        String query2 = String.format("SELECT count(rfq.id) from rfq where rfq.traderId=%s",
                String.valueOf(rfq.getTraderId()));

        trades.createOrReplaceTempView("trade");
        rfqs.createOrReplaceTempView("rfq");
        Dataset<Row> sqlQueryResults1 = session.sql(query1);
        Dataset<Row> sqlQueryResults2 = session.sql(query2);

        Object rfq_resulted_trades = sqlQueryResults1.first().get(0);
        if (rfq_resulted_trades == null) {
            rfq_resulted_trades = 0L;
        }

        Object rfq_by_trader = sqlQueryResults2.first().get(0);
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
