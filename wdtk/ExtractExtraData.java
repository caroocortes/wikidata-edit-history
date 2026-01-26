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

public class ExtractExtraData {
    
    // Static writers for better performance
    private static BufferedWriter labelsWriter;
    private static BufferedWriter p31Writer;
    private static BufferedWriter p279Writer;
    private static BufferedWriter propsWriter;
    
    public static void main(String[] args) throws IOException {

        String labelsFile = "/sc/home/carolina.cortes/wikidata-edit-history/data/entity_labels_alias_description.csv";
        String p31File = "/sc/home/carolina.cortes/wikidata-edit-history/data/p31_entity_types.csv";
        String p279File = "/sc/home/carolina.cortes/wikidata-edit-history/data/p279_entity_types.csv";
        String propertyLabelsFile = "/sc/home/carolina.cortes/wikidata-edit-history/data/property_labels.csv";

        String dumpUrl = "/sc/home/carolina.cortes/Wikidata-Toolkit/dumpfiles/wikidatawiki/json-20251018/wikidata-20251018-all.json.bz2";
        
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
        
        propsWriter = new BufferedWriter(new FileWriter(propertyLabelsFile, true), 131072);
        propsWriter.write("property_id,numeric_id,property_label\n");
        propsWriter.flush();

        controller1.registerEntityDocumentProcessor(new EntityDocumentProcessor() {
            @Override
            public void processItemDocument(ItemDocument item) {
                String qid = item.getEntityId().getId();
                
                // Only cache Q-items
                if (!qid.startsWith("Q")) {
                    return;
                }
                
                // Get label and alias
                String label = getLabel(item);
                String alias = getFirstAlias(item);
                String description = getDescription(item);
                
                // Store in cache
                entityCache.put(qid, new EntityInfo(label, alias, description));
                
                if (count_entities.incrementAndGet() % 100000 == 0) {
                    System.out.println("Cached entities: " + count_entities.get());
                    System.out.println("  Last entity: " + qid + " - " + label);
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
                MonolingualTextValue labelValue = doc.getLabels().get("en");
                return labelValue != null ? labelValue.getText() : "";
            }

            private String getDescription(TermedDocument doc) {
                MonolingualTextValue descriptionValue = doc.getDescriptions().get("en");
                return descriptionValue != null ? descriptionValue.getText() : "";
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

        propsWriter.flush();
        propsWriter.close();
        
        System.out.println("Cache complete! Total entities cached: " + entityCache.size());
        System.out.println("Total properties processed: " + count_properties.get());
        System.out.println();
        
        // =================================================================
        // PASS 2: Write files using the cache
        // =================================================================
        System.out.println("===== PASS 2: Writing files =====");
        
        // Initialize buffered writers (128KB buffer each)
        labelsWriter = new BufferedWriter(new FileWriter(labelsFile, true), 131072);
        p31Writer = new BufferedWriter(new FileWriter(p31File, true), 131072);
        p279Writer = new BufferedWriter(new FileWriter(p279File, true), 131072);
        

        labelsWriter.write("qid,numeric_id,label,alias,description\n");
        p31Writer.write("entity,entity_numeric_id,entity_type,entity_type_numeric_id,label_type,alias,type_qids_list,type_numeric_ids_list,type_labels_list\n");
        p279Writer.write("entity,entity_numeric_id,entity_type,entity_type_numeric_id,label_type,alias,type_qids_list,type_numeric_ids_list,type_labels_list\n");

        // Flush headers
        labelsWriter.flush();
        p31Writer.flush();
        p279Writer.flush();

        System.out.println("Headers written, starting data processing...");
        
        count_entities.set(0);
        count_properties.set(0);
        
        MwLocalDumpFile dump2 = new MwLocalDumpFile(dumpUrl);
        DumpProcessingController controller2 = new DumpProcessingController("wikidatawiki");

        controller2.registerEntityDocumentProcessor(new EntityDocumentProcessor() {

            @Override
            public void processItemDocument(ItemDocument item) {
                try {
                    String qid = item.getEntityId().getId();
                    String numericId = extractNumericId(qid);
                    count_entities.incrementAndGet();

                    // Get this entity's label/alias from cache
                    EntityInfo entityInfo = entityCache.get(qid);
                    String label = entityInfo != null ? entityInfo.label : "";
                    String alias = entityInfo != null ? entityInfo.alias : "";
                    String description = entityInfo != null ? entityInfo.description : "";

                    // Write: qid, numeric_id, label, alias
                    labelsWriter.write(escapeCsv(qid) + "," + 
                            numericId + "," +
                            escapeCsv(label) + "," + 
                            escapeCsv(alias) + "," +
                            escapeCsv(description) + "\n");

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

                    if (!p31Types.isEmpty()) {
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

                        // Write each type as a separate row with aggregated lists
                        for (TypeInfo type : p31Types) {
                            p31Writer.write(escapeCsv(qid) + "," + 
                                    numericId + "," +
                                    escapeCsv(type.id) + "," + 
                                    type.numericId + "," +
                                    escapeCsv(type.label) + "," + 
                                    escapeCsv(type.alias) + "," +
                                    escapeCsv(typeIdsList) + "," +
                                    escapeCsv(typeNumericIdsList) + "," +
                                    escapeCsv(typeLabelsList) + "\n");
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

                    if (!p279Types.isEmpty()) {
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
                            p279Writer.write(escapeCsv(qid) + "," + 
                                    numericId + "," +
                                    escapeCsv(type.id) + "," + 
                                    type.numericId + "," +
                                    escapeCsv(type.label) + "," + 
                                    escapeCsv(type.alias) + "," +
                                    escapeCsv(p279IdsList) + "," +
                                    escapeCsv(p279NumericIdsList) + "," +
                                    escapeCsv(p279LabelsList) + "\n");
                        }
                    }

                    if (count_entities.get() % 10000 == 0) {
                        System.out.println("Progress of entities: " + count_entities.get());
                        
                        labelsWriter.flush();
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
            public void processPropertyDocument(PropertyDocument property) {
                
            }

            @Override
            public void processLexemeDocument(LexemeDocument lexeme) {}

        }, null, true);

        controller2.processDump(dump2); // processes whole dump

        labelsWriter.flush();
        p31Writer.flush();
        p279Writer.flush();
        
        labelsWriter.close();
        p31Writer.close();
        p279Writer.close();

        System.out.println();
        System.out.println("===== Extraction complete =====");
        System.out.println("Total entities processed: " + count_entities.get());
        System.out.println("Total properties processed: " + count_properties.get());

        System.out.println("===== PASS 3: Building transitive closures =====");
    
        Map<String, Set<String>> subclassOf = new ConcurrentHashMap<>();
        Map<String, Set<String>> partOf = new ConcurrentHashMap<>();
        Map<String, Set<String>> hasParts = new ConcurrentHashMap<>();
        Map<String, Set<String>> locatedIn = new ConcurrentHashMap<>();
        Map<String, Set<String>> isMetaclassFor = new ConcurrentHashMap<>();

        // First pass: collect direct relationships
        MwLocalDumpFile dump3 = new MwLocalDumpFile(dumpUrl);
        DumpProcessingController controller3 = new DumpProcessingController("wikidatawiki");
        
        controller3.registerEntityDocumentProcessor(new EntityDocumentProcessor() {
            @Override
            public void processItemDocument(ItemDocument item) {
                String qid = item.getEntityId().getId();
                
                // P279: subclass of
                getPropertyObjects(item, "P279").forEach(obj -> 
                    subclassOf.computeIfAbsent(qid, k -> new HashSet<>()).add(obj)
                );
                
                // P361: part of
                getPropertyObjects(item, "P361").forEach(obj -> 
                    partOf.computeIfAbsent(qid, k -> new HashSet<>()).add(obj)
                );
                
                // P527: has parts
                getPropertyObjects(item, "P527").forEach(obj -> 
                    hasParts.computeIfAbsent(qid, k -> new HashSet<>()).add(obj)
                );
                
                // P131: located in
                getPropertyObjects(item, "P131").forEach(obj -> 
                    locatedIn.computeIfAbsent(qid, k -> new HashSet<>()).add(obj)
                );

                // P8225: is metaclass for
                getPropertyObjects(item, "P8225").forEach(obj -> 
                    isMetaclassFor.computeIfAbsent(qid, k -> new HashSet<>()).add(obj)
                );
            }
            
            private Set<String> getPropertyObjects(ItemDocument item, String propertyId) {
                Set<String> objects = new HashSet<>();
                item.getStatementGroups().stream()
                    .filter(sg -> propertyId.equals(sg.getProperty().getId()))
                    .flatMap(sg -> sg.getStatements().stream())
                    .forEach(statement -> {
                        Value value = statement.getValue();
                        if (value instanceof EntityIdValue) {
                            objects.add(((EntityIdValue) value).getId());
                        }
                    });
                return objects;
            }
            
            @Override
            public void processPropertyDocument(PropertyDocument property) {}
            
            @Override
            public void processLexemeDocument(LexemeDocument lexeme) {}
        }, null, true);
        
        controller3.processDump(dump3); // processes whole dump
        
        // Compute transitive closures (P279+, P361+, etc.)
        System.out.println("Computing transitive closures...");
        Map<String, Set<String>> subclassOfTransitive = computeTransitiveClosure(subclassOf);
        Map<String, Set<String>> partOfTransitive = computeTransitiveClosure(partOf);
        Map<String, Set<String>> hasPartsTransitive = computeTransitiveClosure(hasParts);
        Map<String, Set<String>> locatedInTransitive = computeTransitiveClosure(locatedIn);
        Map<String, Set<String>> isMetaclassForTransitive = computeTransitiveClosure(isMetaclassFor);
        
        System.out.println("Saving transitive relationships...");
        saveRelationships(subclassOfTransitive, "subclass_of_transitive.csv");
        saveRelationships(partOfTransitive, "part_of_transitive.csv");
        saveRelationships(hasPartsTransitive, "has_parts_transitive.csv");
        saveRelationships(locatedInTransitive, "located_in_transitive.csv");
        saveRelationships(isMetaclassForTransitive, "is_metaclass_for_transitive.csv");
        
    }

    private static Map<String, Set<String>> computeTransitiveClosure(Map<String, Set<String>> direct) {
        Map<String, Set<String>> transitive = new ConcurrentHashMap<>();

        /* 
            Example: 
            Cat -> Mammal
            Mammal -> Animal
            Animal -> LivingBeing

        
        */
        
        // Copy direct relationships
        for (Map.Entry<String, Set<String>> entry : direct.entrySet()) {
            transitive.put(entry.getKey(), new HashSet<>(entry.getValue()));

            /* 
                Example: 
                transitive[Cat] =  [Mammal]
                transitive[Mammal] = [Animal]
                transitive[Animal] = [LivingBeing]
            */
        }
        
        // Floyd-Warshall-like algorithm for transitive closure
        boolean changed = true;
        int iteration = 0;
        
        while (changed && iteration < 10) {  // Max 10 hops or the set doesn't change anymore
            changed = false;
            iteration++;
            System.out.println("  Transitive closure iteration " + iteration);

            for (String entity : new HashSet<>(transitive.keySet())) { // go over transitive relationships of the entity and grow from them
                /* 
                    Example - Iteration 1: 
                    transitive[Cat] =  [Mammal]
                    transitive[Mammal] = [Animal]
                    transitive[Animal] = [LivingBeing]

                    For entity = Cat: reachable = [Mammal]
                */
                
                Set<String> reachable = new HashSet<>(transitive.get(entity)); // get what the entity can currently reach
                /* Iteration 1: reachable = [Mammal] */
                for (String intermediate : new HashSet<>(reachable)) {
                    /* Iteration 1: intermediate = Mammal */
                    if (transitive.containsKey(intermediate)) {
                        /* Iteration 1: newReachable = [Animal] (transitive[intermediate] = transitive[Mammal]) */
                        Set<String> newReachable = transitive.get(intermediate);
                        if (reachable.addAll(newReachable)) {
                            /* reachable = [Animal, Mammal] */
                            changed = true;
                        }
                    }
                }
                
                transitive.put(entity, reachable); // Iteration 1: transitive[Cat] = [Mammal, Animal]
            }
        }
        
        return transitive;
    }

    private static String extractNumericId(String id) {
        // Extract numeric part from Q123 -> 123
        if (id != null && id.length() > 1) {
            return id.substring(1);
        }
        return "";
    }

    private static void saveRelationships(Map<String, Set<String>> relationships, String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename), 131072)) {
            writer.write("entity_id, entity_id_numeric, transitive_closure_qids, transitive_closure_numeric_ids\n");
            
            for (Map.Entry<String, Set<String>> entry : relationships.entrySet()) {
                String entity1 = entry.getKey();
                String entity1Numeric = extractNumericId(entity1);
                
                String entity2List = String.join(", ", entry.getValue());
                
                String entity2NumericList = entry.getValue().stream()
                    .map(ExtractExtraData::extractNumericId)
                    .collect(Collectors.joining(", "));
                
                writer.write(entity1 + "," + 
                            entity1Numeric + "," + 
                            "\"" + entity2List + "\"" + "," +
                            "\"" + entity2NumericList + "\"\n");
            }
        }
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

    static class EntityInfo {
        String label;
        String alias;
        String description;
        
        EntityInfo(String label, String alias, String description) {
            this.label = label;
            this.alias = alias;
            this.description = description;
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