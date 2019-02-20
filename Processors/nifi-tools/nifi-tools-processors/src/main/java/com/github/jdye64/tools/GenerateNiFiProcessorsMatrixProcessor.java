package com.github.jdye64.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
*
 */

@Tags({"processors", "generate", "matrix"})
@CapabilityDescription("Generates a NiFi processor support matrix using the input from '/nifi-api/flow/processor-types'. It takes the JSON and formats it in a manner that " +
        "can be consumed by an Excel spreadsheet")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class GenerateNiFiProcessorsMatrixProcessor
        extends AbstractProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateNiFiProcessorsMatrixProcessor.class);

    public static final PropertyDescriptor DELAY_TIME_MS = new PropertyDescriptor
            .Builder().name("Delay Time")
            .description("Number of milliseconds that this processor should delay files before they are allowed to continue through")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to parse the JSON for processor types")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("CSV formatted output that can be opened with Excel/Google Sheets")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DELAY_TIME_MS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        FlowFile ff = session.write(flowFile, new StreamCallback() {

            @Override
            public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                ObjectMapper mapper = new ObjectMapper();
                ProcessorTypesEntity pte = mapper.readValue(inputStream, ProcessorTypesEntity.class);
                if (pte != null) {
                    Set<DocumentedTypeDTO> pts = pte.getProcessorTypes();
                    Iterator<DocumentedTypeDTO> itr = pts.iterator();

                    String header = "Processor Name, Processor Full Name, Nar, Version, Deprecation Notice, Cloudera Supported?, Reason For Unsupported, Customers Requesting\n";
                    outputStream.write(header.getBytes());

                    while (itr.hasNext()) {
                        StringBuilder sb = new StringBuilder();
                        DocumentedTypeDTO dt = itr.next();
                        sb.append(FilenameUtils.getExtension(dt.getType()));
                        sb.append(",");
                        sb.append(dt.getType());
                        sb.append(",");
                        sb.append(dt.getBundle().getArtifact());
                        sb.append(",");
                        sb.append(dt.getBundle().getVersion());
                        sb.append(",");
                        sb.append(dt.getDeprecationReason());
                        sb.append(",");
                        sb.append("YES");
                        sb.append(",");
                        sb.append(",");
                        sb.append("\n");
                        outputStream.write(sb.toString().getBytes());
                    }
                } else {
                    LOGGER.error("ERROR: Parsing JSON containing ProcessorTypeEntity Object");
                }
            }

        });

        session.transfer(ff, REL_SUCCESS);
    }
}
