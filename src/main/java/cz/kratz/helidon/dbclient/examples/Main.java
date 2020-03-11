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

import java.io.IOException;
import java.util.logging.LogManager;

import io.helidon.config.Config;
import io.helidon.dbclient.DbClient;
import io.helidon.dbclient.webserver.jsonp.DbResultSupport;
import io.helidon.media.jsonb.server.JsonBindingSupport;
import io.helidon.media.jsonp.server.JsonSupport;
import io.helidon.tracing.TracerBuilder;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;

/**
 * Java application entry point.
 */
public class Main {
    
    /**
     * Instances of Main utility class are not allowed.
     */
    private Main() {
        throw new UnsupportedOperationException("Instances of Main class are not allowed!");
    }

    /**
     * Application main entry point.
     *
     * @param args Command line arguments. Run with MongoDB support when 1st argument is mongo, run with JDBC support otherwise.
     * @throws java.io.IOException if there are problems reading logging properties
     */
    public static void main(final String[] args) throws IOException {
        // load logging configuration
        LogManager.getLogManager().readConfiguration(Main.class.getResourceAsStream("/logging.properties"));
        // Load default application.yaml configuration file from the classpath
        Config config = Config.create();
        // Get webserver config from the "server" section of application.yaml
        ServerConfiguration serverConfig =
                ServerConfiguration.builder(config.get("server"))
                        .tracer(TracerBuilder.create(config.get("tracing")).build())
                        .build();

        Config dbConfig = config.get("db");
        // Interceptors are added through a service loader - see mongoDB example for explicit interceptors
        DbClient dbClient = DbClient.builder(dbConfig).build();
        // Initialize database schema
        InitializeDb.delete(dbClient);
        InitializeDb.init(dbClient);

        // Prepare routing for the server
        Routing routing = createRouting(config, dbClient);
        WebServer server = WebServer.create(serverConfig, routing);
        // Start the server and print some info.
        server.start().thenAccept(ws -> {
            System.out.println("WEB server is up! http://localhost:" + ws.port() + "/");
        });
        // Server threads are not daemon. NO need to block. Just react.
        server.whenShutdown().thenRun(() -> {
            // Destroy database schema
            InitializeDb.delete(dbClient);
            System.out.println("WEB server is DOWN. Good bye!");
        });
    }

    /**
     * Creates new {@link io.helidon.webserver.Routing}.
     *
     * @param config configuration of this server
     * @return routing configured with JSON support, a health check, and a service
     */
    private static Routing createRouting(Config config, DbClient dbClient) {
        return Routing.builder()
                .register(JsonSupport.create())
                .register(JsonBindingSupport.create())
                .register(DbResultSupport.create())
                .register("/db", new PokemonService(dbClient))
                .build();
    }
}
