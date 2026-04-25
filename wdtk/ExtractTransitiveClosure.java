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

public class ExtractTransitiveClosure {

    private static volatile long maxMemoryUsedMB = 0;
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

    private static void createFileIfNotExists(String filePath) throws IOException {
        File file = new File(filePath);
        file.getParentFile().mkdirs();  // create parent directories if needed
        if (!file.exists()) {
            file.createNewFile();
        }
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
    
    public static void main(String[] args) throws IOException {

        startMemoryMonitor();  
        
        Properties config = new Properties();
        config.load(new FileInputStream("config.properties"));
        outputDir = config.getProperty("output_dir");
        String dumpUrl = config.getProperty("dump_path");

        System.out.println("===== PASS 1: Building transitive closures =====");
    
        Map<String, Set<String>> subclassOf = new ConcurrentHashMap<>();
        Map<String, Set<String>> partOf = new ConcurrentHashMap<>();
        Map<String, Set<String>> hasParts = new ConcurrentHashMap<>();
        Map<String, Set<String>> locatedIn = new ConcurrentHashMap<>();
        // Map<String, Set<String>> isMetaclassFor = new ConcurrentHashMap<>();

        // First pass: collect direct relationships
        MwLocalDumpFile dump2 = new MwLocalDumpFile(dumpUrl);
        DumpProcessingController controller2 = new DumpProcessingController("wikidatawiki");
        
        controller2.registerEntityDocumentProcessor(new EntityDocumentProcessor() {
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
        
        controller2.processDump(dump2); // processes whole dump
        
        // Compute transitive closures (P279+, P361+, etc.)
        System.out.println("Computing transitive closures...");
        Map<String, Set<String>> subclassOfTransitive = computeTransitiveClosure(subclassOf);
        printMemoryStats("subclassOf transitive - size: " + subclassOf.size());
        Map<String, Set<String>> partOfTransitive = computeTransitiveClosure(partOf);
        printMemoryStats("partOf transitive - size: " + partOf.size());
        Map<String, Set<String>> hasPartsTransitive = computeTransitiveClosure(hasParts);
        printMemoryStats("hasParts transitive - size: " + hasParts.size());
        Map<String, Set<String>> locatedInTransitive = computeTransitiveClosure(locatedIn);
        printMemoryStats("locatedIn transitive - size: " + locatedIn.size());
        // Map<String, Set<String>> isMetaclassForTransitive = computeTransitiveClosure(isMetaclassFor);

        System.out.println("Saving transitive relationships...");
        saveRelationships(subclassOfTransitive, "subclass_of_transitive.csv");
        saveRelationships(partOfTransitive, "part_of_transitive.csv");
        saveRelationships(hasPartsTransitive, "has_parts_transitive.csv");
        saveRelationships(locatedInTransitive, "located_in_transitive.csv");
        // saveRelationships(isMetaclassForTransitive, "is_metaclass_for_transitive.csv");

        printMemoryStats("Pass 2 - Transitive closure computation");
        
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
        createFileIfNotExists(outputDir + filename);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + filename), 131072)) {
            writer.write("entity_id, entity_id_numeric, transitive_closure_qids, transitive_closure_numeric_ids\n");
            
            for (Map.Entry<String, Set<String>> entry : relationships.entrySet()) {
                String entity1 = entry.getKey();
                String entity1Numeric = extractNumericId(entity1);
                
                String entity2List = String.join(", ", entry.getValue());
                
                String entity2NumericList = entry.getValue().stream()
                    .map(ExtractTransitiveClosure::extractNumericId)
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
}