package org.wikidata.wdtk.examples;

import org.wikidata.wdtk.dumpfiles.*;
import org.wikidata.wdtk.datamodel.interfaces.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ExtractExtraData {
    public static void main(String[] args) throws IOException {

        String labelsFile = "/PATH_TO_FOLDER/wdtk-output/labels_aliases.csv";
        String p31File = "/PATH_TO_FOLDER/wdtk-output/p31_entity_types.csv";
        String p279File = "/PATH_TO_FOLDER/wdtk-output/p279_entity_types.csv";
        String propertyLabelsFile = "/PATH_TO_FOLDER/wdtk-output/property_labels.csv";

        String dumpUrl = "/PATH_TO_FOLDER/Wikidata-Toolkit/dumpfiles/wikidatawiki/json-20251018/wikidata-20251018-all.json.bz2";
        
        // Cache for entity labels/aliases
        Map<String, EntityInfo> entityCache = new ConcurrentHashMap<>();
        AtomicInteger count_entities = new AtomicInteger(0);
        AtomicInteger count_properties = new AtomicInteger(0);
        
        // =================================================================
        // PASS 1: Build cache of all labels and aliases
        // =================================================================
        System.out.println("===== PASS 1: Building label cache =====");
        
        MwLocalDumpFile dump1 = new MwLocalDumpFile(dumpUrl);
        DumpProcessingController controller1 = new DumpProcessingController("wikidatawiki");
        
        controller1.registerEntityDocumentProcessor(new EntityDocumentProcessor() {
            @Override
            public void processItemDocument(ItemDocument item) {
                String qid = item.getEntityId().getId();
                
                // Get label and alias
                String label = getLabel(item);
                String alias = getFirstAlias(item);
                
                // Store in cache
                entityCache.put(qid, new EntityInfo(label, alias));
                
                if (count_entities.incrementAndGet() % 100000 == 0) {
                    System.out.println("Cached entities: " + count_entities.get());
                }
            }

            @Override
            public void processPropertyDocument(PropertyDocument property) {
                // Cache property labels too
                String pid = property.getEntityId().getId();
                String label = getLabel(property);
                entityCache.put(pid, new EntityInfo(label, ""));
                
                count_properties.incrementAndGet();
            }

            @Override
            public void processLexemeDocument(LexemeDocument lexeme) {
                // no-op
            }
            
            private String getLabel(TermedDocument doc) {
                MonolingualTextValue labelValue = doc.getLabels().get("en");
                return labelValue != null ? labelValue.getText() : "";
            }

            private String getFirstAlias(ItemDocument item) {
                List<MonolingualTextValue> aliases = item.getAliases().get("en");
                if (aliases != null && !aliases.isEmpty()) {
                    return aliases.get(0).getText();
                }
                return "";
            }
        }, null, true);
        
        controller1.processDump(dump1);
        
        System.out.println("Cache complete! Total entities cached: " + entityCache.size());
        System.out.println();
        
        // =================================================================
        // PASS 2: Write files using the cache
        // =================================================================
        System.out.println("===== PASS 2: Writing files =====");
        
        count_entities.set(0);
        count_properties.set(0);
        
        MwLocalDumpFile dump2 = new MwLocalDumpFile(dumpUrl);
        DumpProcessingController controller2 = new DumpProcessingController("wikidatawiki");

        controller2.registerEntityDocumentProcessor(new EntityDocumentProcessor() {

            @Override
            public void processItemDocument(ItemDocument item) {
                String qid = item.getEntityId().getId();
                String numericId = extractNumericId(qid);
                count_entities.incrementAndGet();

                // Get this entity's label/alias from cache
                EntityInfo entityInfo = entityCache.get(qid);
                String label = entityInfo != null ? entityInfo.label : "";
                String alias = entityInfo != null ? entityInfo.alias : "";

                // Write: qid, numeric_id, label, alias
                try (FileWriter fw = new FileWriter(labelsFile, true)) {
                    fw.write(escapeCsv(qid) + "," + 
                            numericId + "," +
                            escapeCsv(label) + "," + 
                            escapeCsv(alias) + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }

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
                            
                            // Get type info from cache
                            EntityInfo typeInfo = entityCache.get(typeId);
                            String typeLabel = typeInfo != null ? typeInfo.label : "";
                            String typeAlias = typeInfo != null ? typeInfo.alias : "";
                            
                            p31Types.add(new TypeInfo(typeId, typeNumericId, typeLabel, typeAlias));
                        }
                    });

                // Build aggregated lists
                String typeIdsList = p31Types.stream()
                    .map(t -> t.id)
                    .collect(Collectors.joining(", "));
                
                String typeNumericIdsList = p31Types.stream()
                    .map(t -> t.numericId)
                    .collect(Collectors.joining(", "));
                    
                String typeLabelsList = p31Types.stream()
                    .map(t -> t.label)
                    .collect(Collectors.joining(", "));

                // Write: entity, entity_numeric_id, entity_type, entity_type_numeric_id, label_type, alias, type_qids_list, type_numeric_ids_list, type_labels_list
                for (TypeInfo type : p31Types) {
                    try (FileWriter fw = new FileWriter(p31File, true)) {
                        fw.write(escapeCsv(qid) + "," + 
                                numericId + "," +
                                escapeCsv(type.id) + "," + 
                                type.numericId + "," +
                                escapeCsv(type.label) + "," + 
                                escapeCsv(type.alias) + "," +
                                escapeCsv(typeIdsList) + "," +
                                escapeCsv(typeNumericIdsList) + "," +
                                escapeCsv(typeLabelsList) + "\n");
                    } catch (IOException e) {
                        e.printStackTrace();
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
                            
                            // Get type info from cache
                            EntityInfo typeInfo = entityCache.get(typeId);
                            String typeLabel = typeInfo != null ? typeInfo.label : "";
                            String typeAlias = typeInfo != null ? typeInfo.alias : "";
                            
                            p279Types.add(new TypeInfo(typeId, typeNumericId, typeLabel, typeAlias));
                        }
                    });

                // Build aggregated lists for P279
                String p279IdsList = p279Types.stream()
                    .map(t -> t.id)
                    .collect(Collectors.joining(", "));
                
                String p279NumericIdsList = p279Types.stream()
                    .map(t -> t.numericId)
                    .collect(Collectors.joining(", "));
                    
                String p279LabelsList = p279Types.stream()
                    .map(t -> t.label)
                    .collect(Collectors.joining(", "));

                // Write each type as a separate row with aggregated lists
                for (TypeInfo type : p279Types) {
                    try (FileWriter fw = new FileWriter(p279File, true)) {
                        fw.write(escapeCsv(qid) + "," + 
                                numericId + "," +
                                escapeCsv(type.id) + "," + 
                                type.numericId + "," +
                                escapeCsv(type.label) + "," + 
                                escapeCsv(type.alias) + "," +
                                escapeCsv(p279IdsList) + "," +
                                escapeCsv(p279NumericIdsList) + "," +
                                escapeCsv(p279LabelsList) + "\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                if (count_entities.get() % 10000 == 0) {
                    System.out.println("Progress of entities: " + count_entities.get());
                }
            }

            @Override
            public void processPropertyDocument(PropertyDocument property) {
                String pid = property.getEntityId().getId();
                String numericId = extractNumericId(pid);
                count_properties.incrementAndGet();
                
                // Get label from cache
                EntityInfo propInfo = entityCache.get(pid);
                String label = propInfo != null ? propInfo.label : "";
                
                // Write: pid, numeric_id, label
                try (FileWriter fw = new FileWriter(propertyLabelsFile, true)) {
                    fw.write(escapeCsv(pid) + "," + 
                            numericId + "," +
                            escapeCsv(label) + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                
                if (count_properties.get() % 1000 == 0) {
                    System.out.println("Progress of properties: " + count_properties.get());
                }
            }

            @Override
            public void processLexemeDocument(LexemeDocument lexeme) {
                // no-op
            }

            private String extractNumericId(String id) {
                // Extract numeric part from Q123 -> 123 or P456 -> 456
                if (id != null && id.length() > 1) {
                    return id.substring(1);
                }
                return "";
            }

            private String escapeCsv(String value) {
                if (value == null || value.isEmpty()) {
                    return "";
                }
                
                if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
                    return "\"" + value.replace("\"", "\"\"") + "\"";
                }
                
                return value;
            }

        }, null, true);

        controller2.processDump(dump2);

        System.out.println();
        System.out.println("===== Extraction complete! =====");
        System.out.println("Total entities processed: " + count_entities.get());
        System.out.println("Total properties processed: " + count_properties.get());
    }

    static class EntityInfo {
        String label;
        String alias;
        
        EntityInfo(String label, String alias) {
            this.label = label;
            this.alias = alias;
        }
    }
    
    static class TypeInfo {
        String id;
        String numericId;
        String label;
        String alias;
        
        TypeInfo(String id, String numericId, String label, String alias) {
            this.id = id;
            this.numericId = numericId;
            this.label = label;
            this.alias = alias;
        }
    }
}