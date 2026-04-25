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

public class ExtractLabelsProperties {
    
    private static BufferedWriter labelsWriter;
    private static BufferedWriter propsWriter;
    private static volatile long maxMemoryUsedMB = 0;
    private static String language;
    private static String outputDir;

    private static void printMemoryStats(String label) {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        long totalMemory = runtime.totalMemory() / (1024 * 1024);
        long maxMemory = runtime.maxMemory() / (1024 * 1024);
        System.out.println("[MEMORY] " + label + 
            " | Used: " + usedMemory + "MB" +
            " | Total: " + totalMemory + "MB" +
            " | Max: " + maxMemory + "MB");
    }

    private static void startMemoryMonitor() {
        Thread monitor = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                Runtime runtime = Runtime.getRuntime();
                long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
                if (usedMemory > maxMemoryUsedMB) {
                    maxMemoryUsedMB = usedMemory;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        monitor.setDaemon(true);
        monitor.start();
    }

    private static void createFileIfNotExists(String filePath) throws IOException {
        File file = new File(filePath);
        file.getParentFile().mkdirs();  // create parent directories if needed
        if (!file.exists()) {
            file.createNewFile();
        }
    }
    
    public static void main(String[] args) throws IOException {

        startMemoryMonitor();  
        long startTime = System.currentTimeMillis();

        Properties config = new Properties();
        config.load(new FileInputStream("config.properties"));
        outputDir = config.getProperty("output_dir");
        language = config.getProperty("language", "en");

        String labelsFile = outputDir + "entity_labels_alias_description.csv";
        String propertyLabelsFile = outputDir + "property_labels.csv";

        createFileIfNotExists(labelsFile);
        createFileIfNotExists(propertyLabelsFile);

        String dumpUrl = config.getProperty("dump_path");
        
        AtomicInteger count_entities = new AtomicInteger(0);
        AtomicInteger count_properties = new AtomicInteger(0);
        
        // =================================================================
        // Extract labels and properties
        // =================================================================
        System.out.println("===== Extract labels and properties =====");
        
        MwLocalDumpFile dump1 = new MwLocalDumpFile(dumpUrl);
        DumpProcessingController controller1 = new DumpProcessingController("wikidatawiki");
        
        propsWriter = new BufferedWriter(new FileWriter(propertyLabelsFile, true), 131072);
        labelsWriter = new BufferedWriter(new FileWriter(labelsFile, true), 131072);

        propsWriter.write("property_id,numeric_id,property_label\n");
        propsWriter.flush();
        labelsWriter.write("qid,numeric_id,label,alias,description\n");
        labelsWriter.flush();

        controller1.registerEntityDocumentProcessor(new EntityDocumentProcessor() {
            @Override
            public void processItemDocument(ItemDocument item) {
                String qid = item.getEntityId().getId();
                
                if (!qid.startsWith("Q")) {
                    return;
                }
                
                // Get label and alias
                String label = getLabel(item);
                String alias = getFirstAlias(item);
                String description = getDescription(item);

                String numericId = extractNumericId(qid);
                
                try{
                    labelsWriter.write(escapeCsv(qid) + "," + 
                                numericId + "," +
                                escapeCsv(label) + "," + 
                                escapeCsv(alias) + "," +
                                escapeCsv(description) + "\n");
                    
                    if (count_entities.incrementAndGet() % 100000 == 0) {
                        System.out.println("Saved entities: " + count_entities.get());
                        System.out.println("  Last entity: " + qid + " - " + label);
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void processPropertyDocument(PropertyDocument property) {
                
                try {
                    String pid = property.getEntityId().getId();
                    String numericId = extractNumericId(pid);
                    count_properties.incrementAndGet();
                    
                    // Get label from property document
                    String label = getLabel(property);
                    
                    // Write: pid, numeric_id, label
                    propsWriter.write(escapeCsv(pid) + "," + 
                        numericId + "," +
                        escapeCsv(label) + "\n");
                    
                    if (count_properties.get() % 1000 == 0) {
                        System.out.println("Progress of properties: " + count_properties.get());
                        System.out.println("  Last property: " + pid + " - " + label);
                        propsWriter.flush();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void processLexemeDocument(LexemeDocument lexeme) {
                // no-op
            }
            
            private String getLabel(TermedDocument doc) {
                MonolingualTextValue labelValue = doc.getLabels().get(language);
                return labelValue != null ? labelValue.getText() : "";
            }

            private String getDescription(TermedDocument doc) {
                MonolingualTextValue descriptionValue = doc.getDescriptions().get(language);
                return descriptionValue != null ? descriptionValue.getText() : "";
            }

            private String getFirstAlias(ItemDocument item) {
                List<MonolingualTextValue> aliases = item.getAliases().get(language);
                if (aliases != null && !aliases.isEmpty()) {
                    return aliases.get(0).getText();
                }
                return "";
            }
        }, null, true);
        
        controller1.processDump(dump1); // processes whole dump

        propsWriter.flush();
        propsWriter.close();

        labelsWriter.flush();
        labelsWriter.close();

        printMemoryStats("Pass 1 - Label & Property extraction");

        long elapsedSec = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println("===== ExtractLabels complete =====");
        System.out.println("Entities: " + count_entities.get());
        System.out.println("Properties: " + count_properties.get());
        System.out.println("Total time: " + elapsedSec + "s (" + (elapsedSec / 60) + "min)");
        System.out.println("Peak memory: " + maxMemoryUsedMB + "MB");
        
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
    
}