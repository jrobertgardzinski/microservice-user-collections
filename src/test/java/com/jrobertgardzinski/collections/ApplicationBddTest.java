package com.jrobertgardzinski.collections;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.FILTER_TAGS_PROPERTY_NAME;
import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;
import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

/**
 * The Gherkin scenarios driven through the USE CASES (application entry point) — every scenario
 * except the {@code @http} refusals, which only exist at the wire (statuses 400/401 have no
 * application-layer counterpart). Its twin {@link HttpBddTest} runs the same feature over the
 * wire; the one feature file, two entry points, is the spec-first pattern from microservice-security.
 */
@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.jrobertgardzinski.collections.appsteps")
@ConfigurationParameter(key = FILTER_TAGS_PROPERTY_NAME, value = "not @http")
@ConfigurationParameter(key = PLUGIN_PROPERTY_NAME,
        value = "pretty, io.qameta.allure.cucumber7jvm.AllureCucumber7Jvm")
class ApplicationBddTest {
}
