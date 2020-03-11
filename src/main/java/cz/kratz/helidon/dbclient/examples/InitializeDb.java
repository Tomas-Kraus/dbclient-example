/*
 * Copyright (c) 2020 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.kratz.helidon.dbclient.examples;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import io.helidon.dbclient.DbClient;
import io.helidon.dbclient.DbExecute;

/**
 * Initialize JDBC database schema and populate it with sample data.
 */
public class InitializeDb {
    
    /** Pokemon types source file. */
    private static final String TYPES = "/Types.json";
    /** Pokemons source file. */
    private static final String POKEMONS = "/Pokemons.json";


    /**
     * Initialize JDBC database schema and populate it with sample data.
     * @param dbClient database client
     */
    static void init(DbClient dbClient) {
        try {
            initSchema(dbClient);
            initData(dbClient);
        } catch (ExecutionException | InterruptedException ex) {
            System.out.printf("Could not initialize database: %s", ex.getMessage());
        }
    }

    /**
     * Delete JDBC database schema.
     * @param dbClient database client
     */
    static void delete(DbClient dbClient) {
        try {
            dbClient.execute(exec -> exec
                    .namedDml("drop-poke-types")
                    .thenCompose(result -> exec.namedDml("drop-pokemons"))
                    .thenCompose(result -> exec.namedDml("drop-types"))
            ).toCompletableFuture().get();
            System.out.printf("Database schema deleted.");
        } catch (ExecutionException | InterruptedException ex1) {
            System.out.printf("Could not drop tables: %s", ex1.getMessage());
        }
    }

    /**
     * Initializes database schema (tables).
     *
     * @param dbClient database client
     */
    private static void initSchema(DbClient dbClient) {
        try {
            dbClient.execute(exec -> exec
                    .namedDml("create-types")
                    .thenCompose(result -> exec.namedDml("create-pokemons"))
                    .thenCompose(result -> exec.namedDml("create-poke-types"))
            ).toCompletableFuture().get();
            System.out.printf("Database schema created.");
        } catch (ExecutionException | InterruptedException ex1) {
            System.out.printf("Could not create tables: %s", ex1.getMessage());
        }
    }

    /**
     * InitializeDb database content (rows in tables).
     *
     * @param dbClient database client
     * @throws ExecutionException when database query failed
     * @throws InterruptedException if the current thread was interrupted
     */
    private static void initData(DbClient dbClient) throws InterruptedException, ExecutionException {
        // Init pokemon types
        dbClient.execute(exec
                -> initTypes(exec)
                        .thenCompose(count -> initPokemons(exec)))
                .toCompletableFuture().get();
    }

    /**
     * Initialize pokemon types.
     * Source data file is JSON file containing array of type objects:
     *<pre>
     * [
     *   { "id": <type_id>, "name": <type_name> },
     *   ...
     * ]
     * </pre>
     * where {@code id} is JSON number and {@ocde name} is JSON String.
     *
     * @param exec database client executor
     * @return executed statements future
     */
    private static CompletionStage<Long> initTypes(DbExecute exec) {
        CompletionStage<Long> stage = null;
        try (JsonReader reader = Json.createReader(InitializeDb.class.getResourceAsStream(TYPES))) {
            JsonArray types = reader.readArray();
            for (JsonValue typeValue : types) {
                JsonObject type = typeValue.asJsonObject();
                stage = stage == null
                        ? exec.namedInsert("insert-type", type.getInt("id"), type.getString("name"))
                        : stage.thenCompose(result -> exec.namedInsert(
                            "insert-type", type.getInt("id"), type.getString("name")));
            }
        }
        return stage;
    }

    /**
     * Initialize pokemos.
     * Source data file is JSON file containing array of type objects:
     *<pre>
     * [
     *   { "id": <type_id>, "name": <type_name>, "type": [<type_id>, <type_id>, ...] },
     *   ...
     * ]
     * </pre>
     * where {@code id} is JSON number and {@ocde name} is JSON String.
     *
     * @param exec database client executor
     * @return executed statements future
     */
    private static CompletionStage<Long> initPokemons(DbExecute exec) {
        CompletionStage<Long> stage = null;
        try (JsonReader reader = Json.createReader(InitializeDb.class.getResourceAsStream(POKEMONS))) {
            JsonArray pokemons = reader.readArray();
            for (JsonValue pokemonValue : pokemons) {
                JsonObject pokemon = pokemonValue.asJsonObject();
                int pid = pokemon.getInt("id");
                stage = stage == null
                        ? exec
                                .namedInsert("insert-pokemon",
                                        pid, pokemon.getString("name"))
                        : stage.thenCompose(result -> exec
                                .namedInsert("insert-pokemon",
                                        pid, pokemon.getString("name")));
                JsonArray type = pokemon.getJsonArray("type");
                for (JsonValue tidValue : type) {
                    stage = stage.thenCompose(result -> exec
                                .namedInsert("insert-poke-types",
                                        pid, ((JsonNumber) tidValue).intValue()));
                }
            }
        }
        return stage;
    }

}
