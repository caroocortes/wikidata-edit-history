package org.wikidata.wdtk.examples;

/*-
 * #%L
 * Wikidata Toolkit Examples
 * %%
 * Copyright (C) 2014 - 2026 Wikidata Toolkit Developers
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.wikidata.wdtk.dumpfiles.*;
import org.wikidata.wdtk.datamodel.interfaces.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ExtractInstanceOfSubclassOf {
    
    private static BufferedWriter p31Writer;
    private static BufferedWriter p279Writer;
    
    public static void main(String[] args) throws IOException {

        Properties config = new Properties();
        config.load(new FileInputStream("config.properties"));
        String outputDir = config.getProperty("output_dir");

        String p31File = outputDir + "p31_entity_types.csv";
        String p279File = outputDir + "p279_entity_types.csv";

        String dumpUrl = config.getProperty("dump_path");
        
        AtomicInteger count_entities = new AtomicInteger(0);

        // =================================================================
        // Extract P31 and P279 types for all entities
        // =================================================================
        System.out.println("===== Extract P31 and P279 types =====");
        
        // Initialize buffered writers (128KB buffer each)
        p31Writer = new BufferedWriter(new FileWriter(p31File, true), 131072);
        p279Writer = new BufferedWriter(new FileWriter(p279File, true), 131072);
        
        p31Writer.write("entity,entity_numeric_id,entity_type,entity_type_numeric_id\n");
        p279Writer.write("entity,entity_numeric_id,entity_type,entity_type_numeric_id\n");

        // Flush headers
        p31Writer.flush();
        p279Writer.flush();

        System.out.println("Headers written, starting data processing...");
        
        count_entities.set(0);
        
        MwLocalDumpFile dump = new MwLocalDumpFile(dumpUrl);
        DumpProcessingController controller2 = new DumpProcessingController("wikidatawiki");

        controller2.registerEntityDocumentProcessor(new EntityDocumentProcessor() {

            @Override
            public void processItemDocument(ItemDocument item) {
                try {
                    String qid = item.getEntityId().getId();
                    String numericId = extractNumericId(qid);
                    count_entities.incrementAndGet();

                    // Collect all P31 types for this entity
                    List<TypeInfo> p31Types = new ArrayList<>();
                    item.getStatementGroups().stream()
                        .filter(sg -> "P31".equals(sg.getProperty().getId()))
                        .flatMap(sg -> sg.getStatements().stream())
                        .forEach(statement -> {
                            Value value = statement.getValue();
                            if (value instanceof EntityIdValue) {
                                String typeId = ((EntityIdValue) value).getId();
                                String typeNumericId = extractNumericId(typeId);
                                
                                p31Types.add(new TypeInfo(typeId, typeNumericId));
                            }
                        });

                    if (!p31Types.isEmpty()) {

                        // Write each type as a separate row
                        for (TypeInfo type : p31Types) {
                            p31Writer.write(
                                escapeCsv(qid) + "," + 
                                numericId + "," +
                                escapeCsv(type.id) + "," + 
                                type.numericId + "\n");
                        }
                    }

                    // Collect all P279 types for this entity
                    List<TypeInfo> p279Types = new ArrayList<>();
                    item.getStatementGroups().stream()
                        .filter(sg -> "P279".equals(sg.getProperty().getId()))
                        .flatMap(sg -> sg.getStatements().stream())
                        .forEach(statement -> {
                            Value value = statement.getValue();
                            if (value instanceof EntityIdValue) {
                                String typeId = ((EntityIdValue) value).getId();
                                String typeNumericId = extractNumericId(typeId);
                                
                                p279Types.add(new TypeInfo(typeId, typeNumericId));
                            }
                        });

                    if (!p279Types.isEmpty()) {

                        // Write each type as a separate row with aggregated lists
                        for (TypeInfo type : p279Types) {
                            p279Writer.write(
                                escapeCsv(qid) + "," + 
                                numericId + "," +
                                escapeCsv(type.id) + "," + 
                                type.numericId + "\n");
                        }
                    }

                    if (count_entities.get() % 10000 == 0) {
                        System.out.println("Progress of entities: " + count_entities.get());
                        
                        p31Writer.flush();
                        p279Writer.flush();

                        if (count_entities.get() % 10000 == 0) {
                            System.out.println("  Last entity: " + qid + " - " + label);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void processPropertyDocument(PropertyDocument property) {}

            @Override
            public void processLexemeDocument(LexemeDocument lexeme) {}

        }, null, true);

        controller2.processDump(dump); // processes whole dump

        p31Writer.flush();
        p279Writer.flush();
        
        p31Writer.close();
        p279Writer.close();

        System.out.println();
        System.out.println("===== Extraction complete =====");
        System.out.println("Total entities processed: " + count_entities.get());
        
    }

    private static String extractNumericId(String id) {
        // Extract numeric part from Q123 -> 123
        if (id != null && id.length() > 1) {
            return id.substring(1);
        }
        return "";
    }

    private static String escapeCsv(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        
        return value;
    }
    
    static class TypeInfo {
        String id;
        String numericId;
        
        TypeInfo(String id, String numericId) {
            this.id = id;
            this.numericId = numericId;
        }
    }
}