/**
* Copyright 2023 Apirus Inc
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.apirus.springbatch.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.apirus.springbatch.listener.JobCompletionNotificationListener;
import com.apirus.springbatch.model.Item;
import com.apirus.springbatch.processor.CsvItemProcessor;
import com.apirus.springbatch.service.ItemFieldSetMapper;
import com.apirus.springbatch.writer.CsvItemWriter;

import lombok.SneakyThrows;
import lombok.extern.slf4j.XSlf4j;

@Configuration
@ConditionalOnProperty(value = "partition", havingValue = "true")
@XSlf4j
public class CsvPartitionProcessConfiguration {
  private static final String[] TOKENS = { "name", "owner", "count", "val1", "val2", "val3", "location", "type",
      "val4" };
  @Bean
  TaskExecutor taskExecutor() {
    // return new SimpleAsyncTaskExecutor("spring_batch");
    ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setMaxPoolSize(5);
    taskExecutor.setCorePoolSize(5);
    taskExecutor.setQueueCapacity(5);
    taskExecutor.afterPropertiesSet();
    return taskExecutor;
  }

  @Bean
  @StepScope
  Resource getInput(@Value("#{jobParameters['file.input']}") FileSystemResource input) {
    return input;
  }

  @Bean
  @StepScope
  FlatFileItemReader<Item> csvItemReader(Resource input, @Value("#{stepExecutionContext['fromLine']}") int fromLine, @Value("#{stepExecutionContext['toLine']}") int toLine) {
    FieldSetMapper<Item> itemFieldSetMapper = new ItemFieldSetMapper();
    log.info("Configuring reader to input {}, fromLine {} - toLine {}", input, fromLine, toLine);
    // @formatter:off
    return new FlatFileItemReaderBuilder<Item>()
      .name("csvItemReader")
      .resource(input)
      .delimited()
      .names(TOKENS)
      .fieldSetMapper(itemFieldSetMapper)
      .currentItemCount(fromLine)
      .maxItemCount(fromLine+2)
      .build();
        // @formatter:on
  }

  @Bean
  @StepScope
  FlatFileItemWriter<Item> csvItemWriter(@Value("#{jobParameters['file.output']}") String output) {
    var fieldExtractor = new BeanWrapperFieldExtractor<Item>();
    fieldExtractor.setNames(TOKENS);
    fieldExtractor.afterPropertiesSet();

    var lineAggregator = new DelimitedLineAggregator<Item>();
    lineAggregator.setDelimiter(",");
    lineAggregator.setFieldExtractor(fieldExtractor);
    return new FlatFileItemWriterBuilder<Item>()
        .name("csvItemWriter")
        .resource(new FileSystemResource(output))
        .lineAggregator(lineAggregator)
        .build();
  }

  @Bean
  @StepScope
  @Primary
  CsvItemWriter consoleItemWriter() {
    return new CsvItemWriter();
  }

  @Bean
  @StepScope
  CsvItemProcessor csvItemProcessor() {
    return new CsvItemProcessor();
  }

  @SneakyThrows
  long countLines(String input) {
    return Files.lines(Paths.get(input), StandardCharsets.UTF_8).count();
  }

  @Bean
  @StepScope
  Partitioner partitioner(@Value("#{jobParameters['file.input']}") String input) {
    log.info("Partitioning file {}", input);
    return gridSize -> {
      var lines = countLines(input);
      var range = (int)(lines / gridSize);
      var remains = lines % gridSize;
      int fromLine = 0;
      int toLine = range;
      var result = new HashMap<String, ExecutionContext>();
      for (int i = 1; i <= gridSize; i++) {
        if (i == gridSize) {
          toLine += remains;
        }
        ExecutionContext value = new ExecutionContext();
        value.putInt("fromLine", fromLine);
        value.putInt("toLine", toLine);
        fromLine = toLine;
        toLine += range;
        result.put("partition" + i, value);
      }
      return result;
    };
  }

  @Bean
  @StepScope
  PartitionHandler partitionHandler(@Qualifier("slaveStep") Step slaveStep, TaskExecutor taskExecutor) {
    TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();

    taskExecutorPartitionHandler.setTaskExecutor(taskExecutor);
    taskExecutorPartitionHandler.setStep(slaveStep);
    taskExecutorPartitionHandler.setGridSize(5);

    return taskExecutorPartitionHandler;
  }

  @Bean("MasterStep1")
  Step step(JobRepository jobRepository, PartitionHandler partitionHandler, Partitioner partitioner) {
    return new StepBuilder("master step 1", jobRepository)
        .partitioner("partition", partitioner)
        .partitionHandler(partitionHandler)
        .build();
  }

  @Bean("slaveStep")
  Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager, FlatFileItemReader<Item> csvItemReader,
      ItemProcessor<Item, Item> csvItemProcessor, ItemWriter<Item> csvItemWriter) {
    log.info("Step1 reader {}");
    // @formatter:off
    return new StepBuilder("step1", jobRepository)
      .<Item, Item> chunk(1, transactionManager)
      .faultTolerant()
      .skipLimit(1)
      .skip(FlatFileParseException.class)
      .reader(csvItemReader)
      .processor(csvItemProcessor)
      .writer(csvItemWriter)
      .build();
    // @formatter:on
  }

  @Bean
  Job job1(JobRepository jobRepository, @Qualifier("MasterStep1") Step masterStep, JobCompletionNotificationListener listener) {
    return new JobBuilder("job1", jobRepository)
        .start(masterStep)
        .listener(listener)
        .build();
  }
}
