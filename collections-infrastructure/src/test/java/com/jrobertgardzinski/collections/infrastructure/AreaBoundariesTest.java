package com.jrobertgardzinski.collections.infrastructure;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A law: the areas inside each layer — core, erasure —
 * reach each other only along the graph written down here.
 *
 * <p>The areas are packages, not modules, so the compiler does not stop {@code domain.core} from
 * importing {@code domain.mfa}; this does. The graph is what the code does today, frozen: a new
 * edge fails the build until somebody adds it here on purpose, and an edge that disappeared fails
 * it too, so the list cannot drift into fiction. Beside the graph two rules hold: a layer reaches
 * only the layers below it (domain, config, system, application, in that order), and no area sits
 * on a cycle.
 */
class AreaBoundariesTest {

    private static final List<String> LAYERS = List.of("domain", "config", "system", "application");

    /** Who may import whom, beyond its own package; a bare {@code application} is that layer's root. */
    private static final Map<String, Set<String>> ALLOWED = Map.ofEntries(
            Map.entry("application.core", Set.of("domain.core", "system.core")),
            Map.entry("domain.erasure", Set.of("domain.core")),
            Map.entry("system.core", Set.of("domain.core")),
            Map.entry("system.erasure", Set.of("config.erasure", "domain.core", "domain.erasure"))
    );

    private static final Pattern PACKAGE = Pattern.compile("^package ([\\w.]+);", Pattern.MULTILINE);
    private static final Pattern REFERENCE = Pattern.compile(
            "\\bcom\\.jrobertgardzinski\\.collections\\.(domain|config|system|application)(?:\\.([a-z]\\w*))?(?=\\.[A-Z])");

    @Test
    @DisplayName("the areas reach each other only along the written graph")
    void the_areas_follow_the_graph() throws IOException {
        Set<String> found = edges(graph());
        Set<String> allowed = edges(ALLOWED);
        Set<String> added = new TreeSet<>(found);
        added.removeAll(allowed);
        Set<String> gone = new TreeSet<>(allowed);
        gone.removeAll(found);
        assertEquals(Set.of(), added, "new couplings between areas — write them into ALLOWED on purpose, or remove them");
        assertEquals(Set.of(), gone, "these edges are written down but the code no longer has them — take them off the list");
    }

    private static Set<String> edges(Map<String, Set<String>> graph) {
        Set<String> edges = new TreeSet<>();
        graph.forEach((from, targets) -> targets.forEach(to -> edges.add(from + " -> " + to)));
        return edges;
    }

    @Test
    @DisplayName("a layer reaches only the layers below it, and no area sits on a cycle")
    void downward_and_acyclic() throws IOException {
        Map<String, Set<String>> graph = graph();
        for (Map.Entry<String, Set<String>> from : graph.entrySet()) {
            int level = LAYERS.indexOf(layerOf(from.getKey()));
            for (String to : from.getValue()) {
                assertTrue(LAYERS.indexOf(layerOf(to)) <= level, from.getKey() + " reaches up, into " + to);
            }
        }
        for (String start : graph.keySet()) {
            assertFalse(reaches(graph, start, start, new TreeSet<>()), start + " sits on a cycle");
        }
    }

    private static boolean reaches(Map<String, Set<String>> graph, String from, String target, Set<String> seen) {
        for (String next : graph.getOrDefault(from, Set.of())) {
            if (next.equals(target) || (seen.add(next) && reaches(graph, next, target, seen))) {
                return true;
            }
        }
        return false;
    }

    private static String layerOf(String area) {
        int dot = area.indexOf('.');
        return dot < 0 ? area : area.substring(0, dot);
    }

    private static Map<String, Set<String>> graph() throws IOException {
        Map<String, Set<String>> graph = new TreeMap<>();
        for (String layer : LAYERS) {
            Path sources = Path.of("../collections-" + layer + "/src/main/java");
            assertTrue(Files.isDirectory(sources), "no sources at " + sources + " — the law would pass on nothing");
            try (Stream<Path> files = Files.walk(sources)) {
                for (Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
                    String source = Files.readString(file);
                    Matcher pkg = PACKAGE.matcher(source);
                    String self = pkg.find() ? areaOf(pkg.group(1) + ".X") : null;
                    if (self == null) {
                        continue;
                    }
                    Matcher reference = REFERENCE.matcher(source);
                    while (reference.find()) {
                        String other = areaOf(reference.group() + ".X");
                        if (!other.equals(self)) {
                            graph.computeIfAbsent(self, key -> new TreeSet<>()).add(other);
                        }
                    }
                }
            }
        }
        return graph;
    }

    /** {@code domain.mfa} for anything in or below that package; {@code application} for that root. */
    private static String areaOf(String name) {
        Matcher m = REFERENCE.matcher(name);
        if (!m.find()) {
            return null;
        }
        return m.group(2) == null ? m.group(1) : m.group(1) + "." + m.group(2);
    }
}
