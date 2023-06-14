package com.apirus.springbatch.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;

public class CsvPartitionProcessConfigurationTest {
    CsvPartitionProcessConfiguration configuration = new CsvPartitionProcessConfiguration();
    @Test
    void testPartitioner() {
        var inputFile = "SampleCSVFile_53000kb.csv";
        var gridSize = 5;
        Map<String, ExecutionContext> expected = Map.of(
            "partition1", new ExecutionContext(Map.of("fromLine",0,"toLine",11901)),
            "partition2", new ExecutionContext(Map.of("fromLine",11901,"toLine",23802)),
            "partition3", new ExecutionContext(Map.of("fromLine",23802,"toLine",35703)),
            "partition4", new ExecutionContext(Map.of("fromLine",35703,"toLine",47604)),
            "partition5", new ExecutionContext(Map.of("fromLine",47604,"toLine",59507))
        );

        var partitioner = configuration.partitioner(Path.of("src","test","resources").resolve(inputFile).toString());
        var result = partitioner.partition(gridSize);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals(expected, result);
    }

}
