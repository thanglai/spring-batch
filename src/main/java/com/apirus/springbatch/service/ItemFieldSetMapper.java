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
package com.apirus.springbatch.service;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import com.apirus.springbatch.model.Item;

public class ItemFieldSetMapper implements FieldSetMapper<Item> {

    @Override
    public Item mapFieldSet(FieldSet fieldSet) throws BindException {
        return Item.builder()
            .name(fieldSet.readString("name"))
            .owner(fieldSet.readString("owner"))
            .count(fieldSet.readString("count"))
            .val1(fieldSet.readString("val1"))
            .val2(fieldSet.readString("val2"))
            .val3(fieldSet.readString("val3"))
            .location(fieldSet.readString("location"))
            .type(fieldSet.readString("type"))
            .val4(fieldSet.readString("val4"))
            .build();
    }

}
