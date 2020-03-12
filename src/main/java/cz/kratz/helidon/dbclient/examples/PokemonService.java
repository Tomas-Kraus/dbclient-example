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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import io.helidon.dbclient.DbClient;
import io.helidon.dbclient.DbRow;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;

/**
 * Example service using a database.
 */
public class PokemonService implements Service {
    
private static final Logger LOGGER = Logger.getLogger(PokemonService.class.getName());

    private final DbClient dbClient;

    PokemonService(DbClient dbClient) {
        this.dbClient = dbClient;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules
                // Print REST service description
                .get("/", this::index)
                // List all pokemons
                .get("/pokemon", this::listPokemons);
    }

    /**
     * Return index page.
     *
     * @param request  the server request
     * @param response the server response
     */
    private void index(ServerRequest request, ServerResponse response) {
        response.send("Pokemon JDBC Example:\n"
        + "     GET /type                - List all pokemon types\n"
        + "     GET /pokemon             - List all pokemons\n"
        + "     GET /pokemon/{id}        - Get pokemon by id\n"
        + "     GET /pokemon/name/{name} - Get pokemon by name\n"
        + "    POST /pokemon             - Insert new pokemon:\n"
        + "                                {\"id\":<id>,\"name\":<name>,\"type\":<type>}\n"
        + "     PUT /pokemon             - Update pokemon\n"
        + "                                {\"id\":<id>,\"name\":<name>,\"type\":<type>}\n"
        + "  DELETE /pokemon/{id}        - Delete pokemon with specified id\n");
    }

        /**
     * Return JsonArray with all stored pokemons.
     * Pokemon object contains list of all type names.
     * This method is abstract because implementation is DB dependent.
     *
     * @param request  the server request
     * @param response the server response
     */
    private void listPokemons(ServerRequest request, ServerResponse response) {
        dbClient.execute(exec -> exec
                .namedQuery("select-all-pokemons")
        ).thenAccept(rows -> {
            ReadPokemonRows.buildJson(dbClient, rows.publisher())
                    .thenAccept(response::send);
        });
    }

    private static final class ReadPokemonRows implements Flow.Subscriber<DbRow> {

        private static CompletionStage<JsonValue> buildJson(
                DbClient dbClient, Flow.Publisher<DbRow> publisher) {
            CompletableFuture<JsonValue> future = new CompletableFuture<>();
            publisher.subscribe(new ReadPokemonRows(dbClient, future));
            return future;
        }

        final DbClient dbClient;
        final JsonArrayBuilder arrayBuilder;
        final CompletableFuture<JsonValue> jsonFuture;
        final AtomicInteger queryCounter;
        final CompletableFuture<Void> queryFuture;
        Flow.Subscription subscription;

        private ReadPokemonRows(DbClient dbClient, CompletableFuture<JsonValue> jsonFuture) {
            this.dbClient = dbClient;
            this.arrayBuilder = Json.createArrayBuilder();
            this.jsonFuture = jsonFuture;
            this.queryCounter = new AtomicInteger(0);
            this.queryFuture = new CompletableFuture<>();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(DbRow row) {
            final JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
            final int pid = row.column("id").as(Integer.class);
            objectBuilder.add("id", pid);
            objectBuilder.add("name", row.column("name").as(String.class));
            queryCounter.incrementAndGet();
            dbClient.execute(exec -> exec
                    .namedQuery("select-type-name-by-pokemon-id", pid)
            ).thenAccept(rows -> {
                rows.collect().thenAccept(rowsList -> {
                    final JsonArrayBuilder typesBuilder = Json.createArrayBuilder();
                    rowsList.forEach(typeRow -> typesBuilder
                            .add(Json.createValue(typeRow.column("name").as(String.class))));
                    objectBuilder.add("type", typesBuilder.build());
                    synchronized(this) {
                        arrayBuilder.add(objectBuilder.build());
                    }
                    if (queryCounter.decrementAndGet() == 0) {
                       queryFuture.complete(null);
                    }
                });
            });
        }

        @Override
        public void onError(Throwable throwable) {
            subscription.cancel();
            jsonFuture.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            System.out.printf("Completed\n");
            queryFuture.thenRun(() -> {
                jsonFuture.complete(arrayBuilder.build());
                System.out.printf("Done\n");
            });
        }

    }

}
