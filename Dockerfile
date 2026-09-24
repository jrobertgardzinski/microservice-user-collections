# user-collections — Helidon 4 SE on the JDK (virtual threads), one jar plus its libs/ classpath
# (built on the host). The jar's manifest Class-Path points at libs/, so it rides next to the jar.
# Build context is the repo root; the paths start at collections-infrastructure because the service
# is four Maven modules (the estate's layers) and infrastructure is the only one with a main().
FROM eclipse-temurin:25-jre-alpine
WORKDIR /app
COPY collections-infrastructure/target/collections-infrastructure.jar app.jar
COPY collections-infrastructure/target/libs libs
EXPOSE 8092
CMD ["java", "-jar", "app.jar"]
