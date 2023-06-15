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

import java.io.IOException;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.apirus.springbatch.listener.JobCompletionNotificationListener;
import com.apirus.springbatch.model.Item;
import com.apirus.springbatch.processor.CsvItemProcessor;
import com.apirus.springbatch.service.ItemFieldSetMapper;
import com.apirus.springbatch.writer.CsvItemWriter;

import lombok.extern.slf4j.XSlf4j;

@Configuration
@ConditionalOnProperty(value = {"async","partition", "simple"}, havingValue = "false", matchIfMissing = true)
@XSlf4j
public class CsvProcessConfiguration {
  private static final String[] TOKENS = { "name", "owner", "count", "val1", "val2", "val3", "location", "type",
      "val4" };

  @Bean
  @StepScope
  FlatFileItemReader<Item> csvItemReader(@Value("#{jobParameters['file.input']}") String input) {
    FlatFileItemReaderBuilder<Item> builder = new FlatFileItemReaderBuilder<>();
    FieldSetMapper<Item> itemFieldSetMapper = new ItemFieldSetMapper();
    log.info("Configuring reader to input {}", input);
    // @formatter:off
        return builder
          .name("csvItemReader")
          .resource(new FileSystemResource(input))
          .delimited()
          .names(TOKENS)
          .fieldSetMapper(itemFieldSetMapper)
          .linesToSkip(59430)
          // .maxItemCount(20)
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

  @Bean
  Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager, ItemReader<Item> csvItemReader,
      ItemProcessor<Item, Item> csvItemProcessor, ItemWriter<Item> csvItemWriter) throws IOException {
    // @formatter:off
        return new StepBuilder("step1", jobRepository)
          .<Item, Item> chunk(3, transactionManager)
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
  Job job1(JobRepository jobRepository, Step step1, JobCompletionNotificationListener listener) {
    return new JobBuilder("job1", jobRepository)
        .start(step1)
        .listener(listener)
        .build();
  }
}
