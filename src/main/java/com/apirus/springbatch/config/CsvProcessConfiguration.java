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

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.apirus.springbatch.model.Item;
import com.apirus.springbatch.service.ItemFieldSetMapper;

import lombok.extern.slf4j.XSlf4j;

@Configuration
@EnableBatchProcessing
@XSlf4j
public class CsvProcessConfiguration {
    private static final String[] TOKENS = { "name", "owner", "count", "val1", "val2", "val3", "location", "type", "val4"};

    @Bean
    @StepScope
    FlatFileItemReader<Item> csvItemReader(@Value("#{jobParameters['file.input']}") String input) {
        FlatFileItemReaderBuilder<Item> builder = new FlatFileItemReaderBuilder<>();
        FieldSetMapper<Item> itemFieldSetMapper = new ItemFieldSetMapper();
        log.info("Configuring reader to input {}", input);
        // @formatter:off
        return builder
          .name("itemItemReader")
          .resource(new FileSystemResource(input))
          .delimited()
          .names(TOKENS)
          .fieldSetMapper(itemFieldSetMapper)
        //   .linesToSkip(59430)
        //   .maxItemCount(20)
          .build();
        // @formatter:on
    }
    
}
