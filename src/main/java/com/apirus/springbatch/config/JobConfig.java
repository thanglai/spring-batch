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
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.extern.slf4j.XSlf4j;

@Configuration
@ConditionalOnProperty(value = "simple", havingValue = "true")
@XSlf4j
class JobConfig {

    @Value(value = "classpath:employees.csv")
    private Resource resource;

    @Bean("MyJob1")
    Job createJob(JobRepository jobRepository, @Qualifier("MyStep1") Step stepMaster) {
        return new JobBuilder("job1", jobRepository)
            // .incrementer(new RunIdIncrementer())
            .start(stepMaster)
            .build();
    }

    @Bean("MyStep1")
    Step step(JobRepository jobRepository, PartitionHandler partitionHandler, Partitioner partitioner) {
        return new StepBuilder("MyStep1", jobRepository)
            .partitioner("slaveStep", partitioner)
            .partitionHandler(partitionHandler)
            .build();
    }

    @Bean("slaveStep")
    Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, FlatFileItemReader<Employee> reader) {
        return new StepBuilder("slaveStep", jobRepository)
            .<Employee, Employee>chunk(1, transactionManager)
            .reader(reader)
            .processor((ItemProcessor<Employee, Employee>) employee -> {
                log.info("Processed item {}", employee);
                return employee;
            })
            .writer(list -> log.info("{}", list))
            .build();
    }
    
    @Bean
    Partitioner partitioner() {
        return gridSize -> {
            Map<String, ExecutionContext> result = new HashMap<>();
            int lines = 0;
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
                while (reader.readLine() != null) lines++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            int range = lines / gridSize;
            int remains = lines % gridSize;
            int fromLine = 0;
            int toLine = range;
            for (int i = 1; i <= gridSize; i++) {
                calculateBuckets(i, lines, range, remains);
                if(i == gridSize) {
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

    void calculateBuckets(int index, int total, int range, int remains) {
        log.info("Calculating index {}, total {}, range {}, remains {}", index, total, range, remains);
        var start = range * index;
        var end = start + range;
        log.info("start {}, end {}", start, end);
    }

    @StepScope
    @Bean
    FlatFileItemReader<Employee> flatFileItemReader(@Value("#{stepExecutionContext['fromLine']}") int startLine, @Value("#{stepExecutionContext['toLine']}") int lastLine) {
        FlatFileItemReader<Employee> reader = new FlatFileItemReader<>();
        reader.setResource(resource);

        DefaultLineMapper<Employee> lineMapper = new DefaultLineMapper<>();
        lineMapper.setFieldSetMapper(fieldSet -> {
            String[] values = fieldSet.getValues();
            return Employee.builder()
                    .id(Integer.parseInt(values[0]))
                    .firstName(values[1])
                    .build();
        });

        lineMapper.setLineTokenizer(new DelimitedLineTokenizer(";"));
        log.info("Setting reader to read from line {} to line {}",startLine, lastLine);
        reader.setLineMapper(lineMapper);
        reader.setCurrentItemCount(startLine);
        reader.setMaxItemCount(lastLine);

        return reader;
    }

    @Bean
    PartitionHandler partitionHandler(@Qualifier("slaveStep") Step step, TaskExecutor taskExecutor) {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();

        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor);
        taskExecutorPartitionHandler.setStep(step);
        taskExecutorPartitionHandler.setGridSize(5);

        return taskExecutorPartitionHandler;
    }

    @Bean
    TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(5);
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setQueueCapacity(5);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }
    
}