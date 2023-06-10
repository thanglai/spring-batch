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
package com.apirus.springbatch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.apirus.springbatch.model.Item;

import lombok.extern.slf4j.XSlf4j;

@XSlf4j
public class CsvItemProcessor implements ItemProcessor<Item, Item> {

    @Override
    @Nullable
    public Item process(@NonNull Item item) throws Exception {
        log.info("Processing {}", item);
        return item;
    }
    
}
