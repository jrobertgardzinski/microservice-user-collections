# TODO — microservice-user-collections

Tylko otwarte rzeczy. Historia = git log.

## Otwarte
- **Kompensacja sagi offboardingu (ADR 0007) — WDROŻONE 2026-08-08.** Ten serwis był ostatnim
  uczestnikiem, który kasował na pierwszą komendę. Dziś: `PURGE_USER_CONTENT` oznacza
  (`PENDING_ERASURE`), `ERASE_USER_CONTENT` kasuje dokładnie zarezerwowane wiersze,
  `RESTORE_USER_CONTENT` cofa. Odczyty przez `active_collection_items`, strażnik
  `ItemReadFilterTest`, alarm `collections_erasure_backlog`. `CollectionStore.purgeUser` USUNIĘTY.
  Otwarte:
  - ~~Alarm zaległości nie ma reguły w Prometheusie~~ — ZROBIONE 2026-08-08:
    reguła `ErasureBacklogStuck` w `../../shared/observability/alert-rules.yml`
    (jedna na trzy serwisy, dopasowanie po sufiksie metryki; `collections_erasure_backlog`).
    **Zostaje**: reguły żyją tylko w stosie compose — wdrożenie k3s nie ma Prometheusa
    (zapisane w `../k8s/README.md` jako dług przyszłego overlaya observability).
  - (opc.) wątek `erasure-backlog-watch` chodzi tylko, gdy jest broker; przy zmianie tej reguły
    pamiętać, że bez brokera nie ma sagi, więc nie ma czego pilnować.
