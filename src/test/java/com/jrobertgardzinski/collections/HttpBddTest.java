package com.jrobertgardzinski.collections;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.FILTER_TAGS_PROPERTY_NAME;
import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;

/**
 * The SAME Gherkin scenarios driven over HTTP (black-box, through a real WebServer). Excludes the
 * {@code @saga} scenario, whose purge arrives over Kafka, not HTTP — that path is covered in the
 * account-deletion slice. Mirrors the multi-entry-point BDD of microservice-security.
 */
@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.jrobertgardzinski.collections.httpsteps")
@ConfigurationParameter(key = FILTER_TAGS_PROPERTY_NAME, value = "not @saga")
class HttpBddTest {
}
