package com.yourorg.loadtest.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resolves a JSON event template for load-test event generation.
 *
 * Accepts three forms of input, tried in this order:
 *  1. An existing filesystem path -- lets you point at a template you're
 *     actively editing, or one that lives outside this repo.
 *  2. An exact classpath resource path (e.g. "events/user-created.json").
 *  3. A short name with no path/extension (e.g. "user-created" or "order-placed"),
 *     resolved against the bundled events/ folder as "events/<name>.json".
 *
 * This lets --event-type stay short for the common case (bundled templates)
 * while still supporting arbitrary external files for anything else.
 */
public final class JsonTemplateLoader {

    private static final String CLASSPATH_TEMPLATE_DIR = "events/";
    private static final String EXTENSION = ".json";

    private final ObjectMapper objectMapper;

    public JsonTemplateLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JsonNode load(String eventTypeOrPath) {
        Path fsPath = Path.of(eventTypeOrPath);
        if (Files.isRegularFile(fsPath)) {
            return parseFile(fsPath);
        }

        JsonNode exact = tryClasspath(eventTypeOrPath);
        if (exact != null) {
            return exact;
        }

        String shortName = eventTypeOrPath.endsWith(EXTENSION) ? eventTypeOrPath : eventTypeOrPath + EXTENSION;
        JsonNode bundled = tryClasspath(CLASSPATH_TEMPLATE_DIR + shortName);
        if (bundled != null) {
            return bundled;
        }

        throw new IllegalStateException(
                "JSON event template not found: '" + eventTypeOrPath + "'. Tried it as a filesystem path, "
                        + "a classpath resource, and as a bundled template name under 'events/'. "
                        + "Add events/" + eventTypeOrPath + ".json, or pass a full path to an external .json file.");
    }

    private JsonNode parseFile(Path fsPath) {
        try {
            return objectMapper.readTree(Files.readString(fsPath));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read/parse JSON template file: " + fsPath, e);
        }
    }

    private JsonNode tryClasspath(String resourcePath) {
        try (InputStream in = JsonTemplateLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                return null;
            }
            return objectMapper.readTree(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read/parse JSON template from classpath: " + resourcePath, e);
        }
    }
}
