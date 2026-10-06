package com.jrobertgardzinski.collections.infrastructure;

import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A law, checked on the compiled classes: inside the layers, an area sees only itself and the
 * shared area, {@value #SHARED}. Areas are packages — core, erasure — so the compiler would let one reach
 * into another; this does not. Beside it the layers point down: domain, config, system,
 * application, each seeing only the ones before it. A layer's root package (no area) is shared too.
 *
 * <p>{@link #ALLOWED_CROSSINGS} names, area to area, the reaches nobody has cut yet. A new one
 * fails the build; one the code no longer has fails it too, so the list can only shrink.
 */
@AnalyzeClasses(packages = "com.jrobertgardzinski.collections",
        importOptions = {ImportOption.DoNotIncludeTests.class, AreaIsolationTest.NoTestJars.class})
class AreaIsolationTest {

    private static final String BASE = "com.jrobertgardzinski.collections";
    private static final List<String> LAYERS = List.of("domain", "config", "system", "application");
    private static final String SHARED = "core";

    /** Reaches from one area into another, as {@code "layer.area -> layer.area"}. */
    private static final Set<String> ALLOWED_CROSSINGS = Set.of();

    @ArchTest
    static final ArchRule an_area_sees_only_itself_and_the_shared_area = classes()
            .that().resideInAnyPackage(LAYERS.stream().map(layer -> BASE + "." + layer + "..").toArray(String[]::new))
            .should(seeOnlyItsOwnAreaAndTheShared());

    @ArchTest
    static final ArchRule layers_point_down = layeredArchitecture().consideringOnlyDependenciesInLayers()
            .layer("domain").definedBy(BASE + ".domain..")
            .layer("config").definedBy(BASE + ".config..")
            .layer("system").definedBy(BASE + ".system..")
            .layer("application").definedBy(BASE + ".application..")
            .whereLayer("domain").mayNotAccessAnyLayer()
            .whereLayer("config").mayOnlyAccessLayers("domain")
            .whereLayer("system").mayOnlyAccessLayers("domain", "config")
            .whereLayer("application").mayOnlyAccessLayers("domain", "config", "system");

    @Test
    void every_allowed_crossing_still_happens() {
        JavaClasses imported = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .withImportOption(new NoTestJars())
                .importPackages(BASE);
        Set<String> found = new TreeSet<>();
        for (JavaClass type : imported) {
            for (Dependency dependency : type.getDirectDependenciesFromSelf()) {
                String crossing = crossing(type, dependency.getTargetClass());
                if (crossing != null) {
                    found.add(crossing);
                }
            }
        }
        Set<String> stale = new TreeSet<>(ALLOWED_CROSSINGS);
        stale.removeAll(found);
        assertEquals(Set.of(), stale, "the code no longer crosses here — take these off ALLOWED_CROSSINGS");
    }

    /**
     * Tests may cross areas — a fake lives beside its port. Inside one module they are left out
     * by {@link ImportOption.DoNotIncludeTests}; another module's tests arrive packaged, as a
     * {@code -tests.jar}, and are left out here.
     */
    static final class NoTestJars implements ImportOption {
        @Override
        public boolean includes(Location location) {
            return !location.contains("-tests.jar");
        }
    }

    private static ArchCondition<JavaClass> seeOnlyItsOwnAreaAndTheShared() {
        return new ArchCondition<>("see only its own area and " + SHARED) {
            @Override
            public void check(JavaClass type, ConditionEvents events) {
                for (Dependency dependency : type.getDirectDependenciesFromSelf()) {
                    String crossing = crossing(type, dependency.getTargetClass());
                    if (crossing != null && !ALLOWED_CROSSINGS.contains(crossing)) {
                        events.add(SimpleConditionEvent.violated(dependency,
                                crossing + ": " + dependency.getDescription()));
                    }
                }
            }
        };
    }

    /** {@code "layer.area -> layer.area"} when the dependency reaches into another, unshared area. */
    private static String crossing(JavaClass from, JavaClass to) {
        String source = areaOf(from);
        String target = areaOf(to.getBaseComponentType());
        if (source == null || target == null) {
            return null;
        }
        String targetArea = target.substring(target.indexOf('.') + 1);
        if (targetArea.equals(SHARED) || targetArea.equals(source.substring(source.indexOf('.') + 1))) {
            return null;
        }
        return source + " -> " + target;
    }

    /** {@code domain.mfa} for a class in or below that package; null outside the layers' areas. */
    private static String areaOf(JavaClass type) {
        String pkg = type.getPackageName();
        for (String layer : LAYERS) {
            String root = BASE + "." + layer + ".";
            if (pkg.startsWith(root)) {
                String rest = pkg.substring(root.length());
                int dot = rest.indexOf('.');
                return layer + "." + (dot < 0 ? rest : rest.substring(0, dot));
            }
        }
        return null;
    }
}
