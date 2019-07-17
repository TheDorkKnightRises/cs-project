package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TotalVolumeTradedByEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    private TotalVolumeTradedByEntityExtractor extractor;
    private Dataset<Row> trades;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);

        extractor = new TotalVolumeTradedByEntityExtractor(new DateTime(2019, 7, 17, 0, 0));

    }

    @Test
    public void checkVolumeForWeekWhenSomeTradesMatch() {

        rfq.setEntityId(5561279226039690843L);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        assertEquals(400_000L, meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastWeek));
    }

    @Test
    public void checkVolumeForMonthWhenSomeTradesMatch() {

        rfq.setEntityId(5561279226039690843L);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        assertEquals(750_000L, meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastMonth));
    }

    @Test
    public void checkVolumeForYearWhenSomeTradesMatch() {

        rfq.setEntityId(5561279226039690843L);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        assertEquals(850_000L, meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastYear));
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {

        // No trades for this entity exist in test data
        rfq.setEntityId(5561279226039690842L);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        assertEquals(0L, meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastYear));
    }

}
