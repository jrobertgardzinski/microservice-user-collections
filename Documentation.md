# microservice-user-collections — Test Report & Documentation

Generated from Allure results by `build_documentation.py` on 2026-08-11. Behaviors below are **verified by passing tests** — rerun the suite, rerun this script, and the document cannot drift from the code.

## 📊 Execution Summary

| Module | Total | Passed | Failed | Broken | Skipped | Duration |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| microservice-user-collections | 142 | 142 | 0 | 0 | 0 | 3.30s |

## 📝 Test Documentation (Behaviors)

This section describes the verified system behaviors based on passing tests.

### Epic: Contract

#### Feature: Account-deletion saga

##### Story: Commands consumed

- **erasesWhatItReservedAndDoesNotConfirm(List)**
- **marksTheLeaversCollectionsAndConfirms(List)**
- **restoresWhatItReservedAndDoesNotConfirm(List)**

##### Story: Confirmation emitted

- **theConfirmationShapeTheOrchestratorReliesOn(PactVerificationContext) microservice-offboarding - a user content purged confirmation**

#### Feature: Cascade events

##### Story: Comments deleted

- **the announcement comments publishes takes the saved references it names**
- **the topic in the pact is the topic this service actually subscribes to**

##### Story: Meme deleted

- **the announcement memes publishes takes every saved reference to the dead meme**
- **the topic in the pact is the topic this service actually subscribes to**

### Epic: Domain

#### Feature: Saved item lifecycle

- **a fresh reference is ACTIVE and carries no mark**
- **a mark without its instant — or an instant without its mark — cannot be built**
- **a redelivered mark keeps the FIRST instant — the backlog measures an age**
- **marking reserves the reference and records when**
- **restoring a reference nobody marked is a no-op, not an error**
- **restoring puts it back exactly as it was, and twice is once**

### Epic: Executable specs

#### Feature: An account deletion empties the COLLECTIONS — carefully

- **A closure for a SAGA that reserved nothing destroys nothing**
- **A failure at another participant brings the saved list back**
- **A purge naming nobody is ignored**
- **The closure is the point of no return**
- **The purge empties every COLLECTION at once and answers the ORCHESTRATOR**

#### Feature: Listing a COLLECTION

- **COLLECTIONS are per person and per name**
- **For a GUEST there are no COLLECTIONS**
- **The newest save is listed first**

#### Feature: Removing a REFERENCE

- **A removed REFERENCE is gone from the COLLECTION**
- **Removing what was never there is told, not punished**

#### Feature: Saving a REFERENCE

- **A REFERENCE too long to be real is refused at the door**
- **A saved REFERENCE lands in the COLLECTION**
- **Saving twice is not an error — the caller is told it was already there**

### Epic: Infrastructure

#### Feature: Collection persistence

- **a_second_mark_keeps_the_first_instant_so_a_redelivery_cannot_rejuvenate_the_backlog()**
- **collections_are_scoped_per_user_and_name()**
- **lists_newest_first()**
- **only_the_closure_deletes_and_only_what_the_mark_reserved()**
- **remove_takes_it_out_and_is_idempotent()**
- **save_is_idempotent()**
- **the_compensation_puts_the_lists_back_exactly_as_they_were()**
- **the_mark_empties_every_list_of_the_leaver_and_deletes_nothing()**

##### Story: Item axis and its index

- **a_batch_larger_than_one_statements_worth_of_ids_still_purges_all_of_it()**
- **purging_an_item_takes_every_users_ref_to_it_and_nothing_else()**
- **purging_an_item_twice_removes_nothing_the_second_time()**
- **the_migrations_leave_an_index_on_the_item_axis()**
- **the_v1_user_index_survives_the_new_migration()**

#### Feature: Cross-origin requests

- **a_foreign_origin_gets_no_cors_headers_at_all()**
- **a_preflight_from_an_allowed_origin_is_answered_without_touching_the_routes()**
- **an_actual_request_carries_the_origin_echo()**

#### Feature: Database timeouts

- **the_connection_timeouts_reach_hikari_and_the_driver()**
- **the_in_memory_h2_gets_hikaris_clock_but_none_of_the_postgres_properties()**
- **the_socket_timeout_reaches_the_postgres_driver_properties()**
- **the_statement_timeout_rides_along_as_a_session_option()**

#### Feature: Deletion cascade

##### Story: Consumer loop

- **a_permanently_broken_store_leaves_a_dead_row_instead_of_wedging_the_partition()**
- **a_poison_pill_is_committed_away_rather_than_retried()**
- **a_transient_store_failure_is_retried_and_the_cascade_still_happens()**
- **an_interrupt_during_the_retry_backoff_ends_the_loop()**
- **one_loop_serves_both_cascade_topics_and_commits_what_it_handled()**
- **the_correlation_id_rides_from_the_header_into_the_mdc()**

##### Story: Event handling

- **a_comments_cascade_logs_the_meme_it_belonged_to()**
- **a_comments_deleted_with_an_empty_list_removes_nothing_and_raises_nothing()**
- **a_comments_deleted_without_a_usable_meme_id_is_dropped_with_a_warning()**
- **a_deleted_meme_does_not_take_the_comment_that_shares_its_id()**
- **a_deleted_meme_loses_every_users_reference_to_it()**
- **a_foreign_event_type_on_either_topic_is_ignored_without_a_word()**
- **a_malformed_payload_is_dropped_and_never_echoed()**
- **a_meme_deleted_without_a_usable_id_is_dropped_with_a_warning()**
- **a_null_payload_is_a_drop_not_a_crash()**
- **a_successful_cascade_logs_the_meme_id_and_the_number_of_refs()**
- **an_event_of_the_right_type_on_the_wrong_topic_is_not_ours()**
- **deleted_comments_lose_exactly_the_refs_the_event_names()**
- **the_cascade_does_not_touch_the_saga_topics()**
- **the_cascade_runs_in_its_own_consumer_group()**
- **the_saga_confirmation_sharing_the_comments_topic_never_leaks_its_email()**
- **the_same_meme_deletion_twice_is_free()**
- **unusable_ids_inside_a_comments_deleted_are_skipped_and_the_rest_still_go()**

##### Story: Topic names

- **a cascade event that arrives on any other topic is not acted on**
- **the cascade's topic constants are the names the producers publish on**
- **the running listener subscribes to exactly those two topics — nothing else**

#### Feature: HTTP API

##### Story: Refusals at the edge

- **a_collection_wider_than_the_schema_column_answers_400()**
- **a_garbage_bearer_token_answers_401()**
- **a_non_bearer_scheme_answers_401()**
- **an_item_id_wider_than_the_schema_column_answers_400()**
- **the_widest_fitting_item_id_is_still_accepted()**
- **without_authorization_every_route_answers_401()**

#### Feature: Health probes

- **a_fresh_consumer_is_alive()**
- **a_fresh_consumer_is_healthy()**
- **a_marker_older_than_the_tolerance_reports_a_stall()**
- **a_marker_within_the_tolerance_stays_healthy()**
- **a_negative_stall_env_refuses_to_start_for_either_variable()**
- **a_scheduled_marker_older_than_the_tolerance_reports_an_alive_stall()**
- **a_stalled_cycle_with_a_fresh_schedule_is_alive_but_not_healthy()**
- **a_zero_stall_env_refuses_to_start_naming_the_variable_and_the_value()**
- **an_alive_stall_at_or_above_the_derived_floor_is_kept()**
- **an_alive_stall_below_the_derived_floor_is_floored()**
- **an_unparseable_stall_env_names_the_variable_and_the_value()**
- **the_alive_floor_covers_the_whole_worst_legal_iteration()**
- **the_batch_is_capped_so_a_drained_backlog_cannot_outrun_the_poll_interval()**
- **the_code_default_sits_above_the_floor_instead_of_being_corrected_by_it()**
- **the_consumers_own_blocking_clock_is_explicit_and_inside_the_alive_floor()**
- **the_database_clocks_are_a_term_of_the_floor_not_an_unbounded_wait()**

#### Feature: Offline token verification

- **accepts_a_valid_token_and_reads_the_user()**
- **rejects_a_foreign_issuer()**
- **rejects_a_tampered_signature()**
- **rejects_a_token_signed_by_an_unknown_key()**
- **rejects_an_expired_token()**

### Epic: Saga

#### Feature: Erasure backlog alarm

- **a mark older than any saga can last is counted and said out loud**
- **a register that cannot be read keeps its last value instead of reporting zero**
- **a saga still running is not an alarm — the marks are minutes old, not hours**
- **the closure landing clears the alarm by itself — that is why it is a gauge**

#### Feature: Marked rows stay hidden

- **no query outside the erasure adapter reads the collection_items table directly**
- **the exemption is earned: the erasure adapter really does read the table**

#### Feature: Purge commands

##### Story: Command handling

- **a_command_of_another_type_is_ignored()**
- **a_malformed_command_is_dropped_not_thrown()**
- **a_malformed_payload_is_not_echoed_into_the_log()**
- **a_purge_command_clears_the_user_and_confirms()**
- **a_purge_command_without_an_email_is_dropped_without_a_confirmation()**
- **a_successful_purge_logs_the_saga_id_never_the_email()**

##### Story: Consumer loop

- **a_commit_that_keeps_failing_retries_without_end_and_drops_nothing()**
- **a_dead_broker_on_a_quiet_topic_fails_the_probe_and_stalls_health_not_alive()**
- **a_failed_probe_rewinds_nothing_and_keeps_the_liveness_beat()**
- **a_finished_thread_lets_the_alive_marker_stall()**
- **a_permanently_failing_store_keeps_alive_green_while_health_stalls()**
- **a_processed_record_is_confirmed_before_its_offset_commits()**
- **a_record_still_failing_when_its_budget_ends_is_dropped_loudly_committed_and_counted()**
- **a_store_failure_inside_the_budget_commits_nothing_and_the_loop_retries_after_backoff()**
- **an_empty_email_is_dropped_without_confirmation_but_with_commit()**
- **an_interrupt_mid_send_ends_the_loop_instead_of_being_swallowed()**
- **the_confirmation_is_keyed_by_the_saga_never_by_the_leavers_address()**
- **the_probe_is_paced_by_the_clock_not_by_the_cycle_count()**

### Epic: Use case

#### Feature: Idempotent commands

- **close a saga that marked nothing**
- **close the saga on a marked account**
- **compensate a marked account**
- **compensate a saga that marked nothing**
- **mark the account for erasure**
- **purge a deleted item everyone's collections may point at**
- **purge a deleted item nobody saved**
- **remove what is not there**
- **remove what is there**
- **save into an empty collection**
- **save what is already there**

#### Feature: Purge deleted item

- **a_batch_removes_exactly_the_named_ids()**
- **an_item_nobody_saved_is_zero_not_a_failure()**
- **blanks_and_duplicates_in_one_event_cost_nothing()**
- **it_removes_the_deleted_item_from_every_user_and_every_collection()**
- **it_takes_nobody_elses_refs_with_it()**
- **nothing_worth_purging_is_zero_rather_than_an_empty_statement()**
- **running_it_again_removes_nothing_and_raises_nothing()**

