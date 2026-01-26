package org.wikidata.wdtk.examples;

/*-
 * #%L
 * Wikidata Toolkit Examples
 * %%
 * Copyright (C) 2014 - 2025 Wikidata Toolkit Developers
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
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.io.FileOutputStream;
import java.io.File;

public class ExtractExtraData {
    public static void main(String[] args) throws IOException {

        String labelsFile = "/PATH_TO_FOLDER/wdtk-output/labels_aliases.csv";
        String p31File = "/PATH_TO_FOLDER/wdtk-output/p31.csv";
        String p279File = "/PATH_TO_FOLDER/wdtk-output/p279.csv";
        String propertyLabelsFile = "/PATH_TO_FOLDER/wdtk-output/property_labels.csv";

        String dumpUrl = "/PATH_TO_FOLDER/Wikidata-Toolkit/dumpfiles/wikidatawiki/json-20251018/wikidata-20251018-all.json.bz2";
        MwLocalDumpFile dump = new MwLocalDumpFile(dumpUrl);
        DumpProcessingController controller = new DumpProcessingController("wikidatawiki");

        AtomicInteger count_entities = new AtomicInteger(0);
        AtomicInteger count_properties = new AtomicInteger(0);

        controller.registerEntityDocumentProcessor(new EntityDocumentProcessor() {

            @Override
            public void processItemDocument(ItemDocument item) {
                String qid = item.getEntityId().getId();

                count_entities.incrementAndGet();

                // Labels (en)
                MonolingualTextValue labelValue = null;
                if (item.getLabels().containsKey("en")) {
                    labelValue = item.getLabels().get("en");
                }
                String label = labelValue != null ? labelValue.getText() : "";

                // Aliases (en)
                List<MonolingualTextValue> aliases = item.getAliases().get("en");
                String alias = "";
                if (aliases != null && !aliases.isEmpty()) {
                    alias = aliases.get(0).getText().replace(";", " ");
                }

                // Write label and alias 
                try (FileWriter fw = new FileWriter(labelsFile, true)) {
                    fw.write(qid + ";" + label.replace(";", " ") + ";" + alias + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // P31 (instance of)
                item.getStatementGroups().stream()
                    .filter(sg -> "P31".equals(sg.getProperty().getId()))
                    .flatMap(sg -> sg.getStatements().stream())
                    .forEach(statement -> {
                        Value value = statement.getValue();
                        if (value instanceof EntityIdValue) {
                            String p31Id = ((EntityIdValue) value).getId();
                            String rank = statement.getRank().toString(); // NORMAL, PREFERRED, or DEPRECATED
                            
                            try (FileWriter fw = new FileWriter(p31File, true)) {
                                fw.write(qid + ";" + p31Id + ";" + rank + "\n");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });

            // P279 (subclass of) 
            item.getStatementGroups().stream()
                .filter(sg -> "P279".equals(sg.getProperty().getId()))
                .flatMap(sg -> sg.getStatements().stream())
                .forEach(statement -> {
                    Value value = statement.getValue();
                    if (value instanceof EntityIdValue) {
                        String p279Id = ((EntityIdValue) value).getId();
                        String rank = statement.getRank().toString(); // NORMAL, PREFERRED, or DEPRECATED
                        
                        try (FileWriter fw = new FileWriter(p279File, true)) {
                            fw.write(qid + ";" + p279Id + ";" + rank + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });

                if (count_entities.get() % 10000 == 0) {
                    System.out.println("Progress of entities: " + count_entities);
                }
            }

            @Override
            public void processPropertyDocument(PropertyDocument property) {
                String pid = property.getEntityId().getId();
                
                count_properties.incrementAndGet();
                
                // Get English label
                MonolingualTextValue labelValue = null;
                if (property.getLabels().containsKey("en")) {
                    labelValue = property.getLabels().get("en");
                }
                String label = labelValue != null ? labelValue.getText() : "";
                
                // Write to file
                try (FileWriter fw = new FileWriter(propertyLabelsFile, true)) {
                    fw.write(pid + ";" + label.replace(";", " ") + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                
                if (count_properties.get() % 1000 == 0) {
                    System.out.println("Progress of properties: " + count_properties);
                }
            }

            @Override
            public void processLexemeDocument(LexemeDocument lexeme) {
                // no-op
            }
        }, null, true);

        controller.processDump(dump);

        System.out.println("Extraction complete!");
    }
}
