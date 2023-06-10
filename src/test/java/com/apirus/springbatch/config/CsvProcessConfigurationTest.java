package com.apirus.springbatch.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.slf4j.profiler.Profiler;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.test.MetaDataInstanceFactory;

import com.apirus.springbatch.model.Item;

class CsvProcessConfigurationTest {
    CsvProcessConfiguration csvProcessConfiguration = new CsvProcessConfiguration();

    @Test
    void testCsvItemReader() throws URISyntaxException {
        var profiler = new Profiler(getClass().getSimpleName());
        profiler.start("testCsvItemReader");
        var EXPECTED_COUNT = 20;
        var inputFile = "SampleCSVFile_53000kb.csv";
        var csvItemReader = csvProcessConfiguration.csvItemReader(Paths.get(getClass().getClassLoader().getResource(inputFile).toURI()).toString());
        // read
        int error = 0;
        try {
            csvItemReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
            int count = 0;
            Item line;
            while ((line = csvItemReader.read()) != null) {
                // assertEquals(String.valueOf(count), line);
                if (count < 10 || count > 59430)
                    System.out.printf("%d %s%n", count, line);
                count++;
            }
            assertEquals(EXPECTED_COUNT, count);
        } catch (FlatFileParseException e) {
            error++;
        } catch (Exception e) {
            fail(e.getMessage(), e);
        } finally {
            System.err.println(error);
            csvItemReader.close();
            profiler.stop().print();
        }
    }
}
