package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TotalVolumeTradedForInstrumentExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
    }

    @Test
    public void checkVolumeWhenSomeTradesMatch() {

        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        rfq.setIsin("AT0000A0VRQ6");
        TotalVolumeTradedForInstrumentExtractor extractor = new TotalVolumeTradedForInstrumentExtractor();
        extractor.setSince("2019-06-17");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastMonth);

        assertEquals(750_000L, result);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {

        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for AT0000A0VRQ6 so this will cause no matches
        rfq.setIsin("AT0000A0VRQ7");
        TotalVolumeTradedForInstrumentExtractor extractor = new TotalVolumeTradedForInstrumentExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastMonth);

        assertEquals(0L, result);
    }

}
