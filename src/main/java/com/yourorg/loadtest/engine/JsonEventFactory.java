package com.yourorg.loadtest.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Generates one JSON event per call from a template, by walking the template
 * tree and substituting any recognized {{token}} found inside string values.
 *
 * Supported tokens (extend the TOKEN_GENERATORS map to add more):
 *  - {{uuid}}            -> a fresh random UUID, e.g. "3fa85f64-5717-4562-..."
 *  - {{timestampMillis}} -> current epoch millis as a string
 *  - {{randomInt}}       -> a random int in [0, 100000)
 *
 * A string value that is EXACTLY one token (e.g. "{{uuid}}") is replaced with
 * the generated value. Tokens embedded inside a longer string (e.g.
 * "order-{{uuid}}") are also substituted, so templates can mix static and
 * dynamic content freely.
 */
public class JsonEventFactory {

    private static final Random RANDOM = new Random();

    private static final Map<String, Supplier<String>> TOKEN_GENERATORS = Map.of(
            "uuid", () -> UUID.randomUUID().toString(),
            "timestampMillis", () -> String.valueOf(Instant.now().toEpochMilli()),
            "randomInt", () -> String.valueOf(RANDOM.nextInt(100_000))
    );

    private final JsonNode template;

    public JsonEventFactory(JsonNode template) {
        this.template = template;
    }

    public JsonNode newEvent() {
        return substitute(template.deepCopy());
    }

    private JsonNode substitute(JsonNode node) {
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            Iterator<Map.Entry<String, JsonNode>> fields = obj.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                entry.setValue(substitute(entry.getValue()));
            }
            return obj;
        }
        if (node.isArray()) {
            ArrayNode array = (ArrayNode) node;
            for (int i = 0; i < array.size(); i++) {
                array.set(i, substitute(array.get(i)));
            }
            return array;
        }
        if (node.isTextual()) {
            return new TextNode(substituteTokens(node.asText()));
        }
        return node; // numbers, booleans, null -- nothing to substitute
    }

    private String substituteTokens(String text) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            int open = text.indexOf("{{", i);
            if (open < 0) {
                result.append(text, i, text.length());
                break;
            }
            result.append(text, i, open);
            int close = text.indexOf("}}", open);
            if (close < 0) {
                // Unmatched "{{" -- leave the rest as-is rather than silently dropping it.
                result.append(text.substring(open));
                break;
            }
            String tokenName = text.substring(open + 2, close).trim();
            Supplier<String> generator = TOKEN_GENERATORS.get(tokenName);
            if (generator != null) {
                result.append(generator.get());
            } else {
                // Unknown token -- fail loudly rather than silently sending "{{typo}}"
                // as literal event data.
                throw new IllegalArgumentException(
                        "Unknown template token '{{" + tokenName + "}}'. Supported tokens: "
                                + TOKEN_GENERATORS.keySet());
            }
            i = close + 2;
        }
        return result.toString();
    }
}
