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

- **Zapis po starcie kasowania — dziura estate'owa, ZWĘŻONA i WIDOCZNA, nie zamknięta.** Bramka
  tutaj jest offline, więc token odchodzącego jest przyjmowany aż do własnego `exp` (domyślnie
  godzina) po `POST /account/delete`. Zrobione po stronie tego serwisu: (1) `DELETE` nie rusza
  wiersza zarezerwowanego przez sagę — kasowanie z nieświeżej karty nie odbierze kompensacji
  czego przywrócić; (2) zamknięcie liczy, ile referencji musiało zostawić pod adresem, który
  własnie wymazało — `collections_erasure_residue_total` + WARN z sagą (nie kasuje ich: ten sam
  adres mógł już wziąć ktoś inny, a hurtowe kasowanie przy ponownym doręczeniu zamknięcia
  wyczyściłoby JEGO listę). **Zostaje**: zapis PO zamknięciu, w resztce ważności tokenu, nie ma
  kto zobaczyć — potrzebny sygnał unieważnienia z security (np. na `security-events`, obok
  `EMAIL_CHANGED`) albo pytanie o token per żądanie. To samo dotyczy memes i comments —
  decyzja estate'owa, nie tego repo.
