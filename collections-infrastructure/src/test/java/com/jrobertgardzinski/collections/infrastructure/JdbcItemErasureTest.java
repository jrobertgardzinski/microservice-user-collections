package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.ItemErasure;
import com.jrobertgardzinski.collections.application.ItemErasureContractTest;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.identity.UserId;

import java.util.Optional;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;

import java.util.UUID;

/**
 * The REFERENCE: the adapter the service actually runs on, against a real database migrated by
 * Flyway. Without it the contract would only prove that the stand-ins agree with each other.
 *
 * <p>A fresh in-memory database per test, the same way {@code JdbcCollectionRepositoryTest} does it —
 * the contract also protects itself with per-run addresses, and the two together mean this suite
 * cannot be ordered into failing.
 */
@Epic("Architecture")
@Feature("A stand-in behaves like the adapter it stands in for")
class JdbcItemErasureTest extends ItemErasureContractTest {

    private JdbcCollectionRepository store;
    private JdbcItemErasure erasure;

    @BeforeEach
    void migrateFreshDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:contract_" + UUID.randomUUID()
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        HikariDataSource dataSource = new HikariDataSource(config);
        Flyway.configure().dataSource(dataSource).load().migrate();
        store = new JdbcCollectionRepository(dataSource);
        erasure = new JdbcItemErasure(dataSource);
    }

    @Override
    protected ItemErasure erasure() {
        return erasure;
    }

    @Override
    protected void givenSavedItem(String user, Optional<UserId> userId, String collection, ItemRef ref) {
        store.add(user, userId, collection, ref);
    }
}
