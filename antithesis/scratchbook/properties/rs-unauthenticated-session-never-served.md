# rs-unauthenticated-session-never-served — pull's replication source never sends binlog data, resultsets or OK to a client that did not authenticate

## What led here

Security focus: authentication on pull's home-grown MySQL server (`minimysql`). It serves **decrypted** binlogs (the only in-product decryption path) over cleartext TCP, guarded by one configured user and password. The handshake is hand-written (greeting → optional auth-method switch → check → fast-auth + OK → command loop). All sessions share one io thread, and per-session state lives in a `connection_context`. The property guards against any sequence (malformed greeting, auth-switch games, mid-handshake resets, interleaved sessions) that reaches the command loop without a passing `check_client_authentication`.

## Code paths

- `src/minimysql/network_service.cpp:344-455` `session`: straight-line handshake. Returns after "access denied" if the client lacks `plugin_auth` (`:404-415`) or if `check_client_authentication()` fails (`:443-455`). The command loop (`:553-620`), and with it `handle_binlog_dump_command` (`:597-602`), is reachable only after `:443`. Any exception ends the session (`:621-623`).
- `src/minimysql/connection_context.cpp:111-117` `check_client_authentication`: username equality and `scramble(password, server_auth_method_data_)` equality.
- `:487-514` scramble: 20 bytes from the OpenSSL CSPRNG, per connection; refreshed on auth switch (`:203-226`), and `parse_client_auth_method_switch` (`:228-246`) forces `client_auth_method_ = server_auth_method_`.
- `:520-527` strict sequence-number validation; mismatches throw.
- `src/binsrv/authentication_config.cpp:25-45`: user and password must be non-empty and the plugin must be `caching_sha2_password`, so an empty-password bypass is ruled out at config load.
- The comparison (`std::ranges::equal`) is not constant-time. The per-connection random salt makes a timing oracle useless, so this is not a property.
- `connection_context` is per session; the only cross-session state is the atomic `next_connection_id_` (`:454`). No shared mutable auth state was found.

## Failure scenario (hypothetical; regression guard)

A refactor adds a pre-auth fast path (for example handling `COM_PING`/`COM_QUIT` before auth, or TLS upgrade), or reuses a context across sessions, and a client that sent a wrong scramble reaches the command loop. A second scenario: the auth-switch branch is taken, the client resets mid-switch, and a bug leaves the context marked authenticated. Today the code is linear and no violation was found by reading. Confidence the property holds: high.

## Oracle

- Workload "attacker" clients, concurrent with legitimate `mysqlbinlog` dumps: wrong password, wrong user, correct password but scramble against the *pre-switch* salt, greeting without `plugin_auth`, `mysql_native_password` requested, truncated greeting, reset mid-switch, and greeting bytes replaced by a COM_BINLOG_DUMP frame.
- For each attacker session: `Always(no frame received after the greeting other than AuthMethodSwitch or an ERR packet with code 1045; a silent close is also allowed)`. Specifically never an OK (0x00), never fast-auth (0x01 0x03), and never a binlog event.
- SUT-side (missing): an `authenticated` flag on `connection_context`, set only at `network_service.cpp:457`, and `ALWAYS(context.authenticated)` at the top of the command loop and in `handle_binlog_dump_command`.
- `Sometimes(attacker session hit the auth-switch path)` so the branch is known to be explored.

## Antithesis angle

Many interleaved sessions on the single io thread, packet drops and reordering mid-handshake, and resets between the switch request and the response. Low expected yield: this is a cheap invariant that guards the boundary that protects decrypted data.

## Instrumentation

- missing: SUT-side ALWAYS and the `authenticated` flag.
- exists (non-Antithesis): `extra/qa/listener_fuzz_test.py` (sequential malformed traffic, clean-handshake check).

## Investigation outcome (2026-10-13)

- Re-verified by reading `src/minimysql/network_service.cpp:344-623`. Before `check_client_authentication()` succeeds (`:443`), the only frames a client can receive are the greeting, `AuthMethodSwitch` (`:417-431`), and ERR `ER_ACCESS_DENIED_ERROR` (1045, SQLSTATE 28000; `connection_context.cpp:320-324`) on the paths at `:404-415` / `:443-455`. Fast-auth (`:460-470`) and the auth OK (`:472-478`) come only after success. COM_PING's OK and COM_BINLOG_DUMP exist only inside the command loop (`:553-620`). Any parse or sequence exception closes the socket without a frame (`:621-623`). **Oracle refinement:** "no further frame (socket closed)" is also an allowed attacker outcome. The allowed set is {AuthMethodSwitch, ERR 1045, close}.
- No change to the property. The remaining question concerns a possible future TLS clause, not today's invariant.

## Open questions

- Is the shared-password, cleartext listener the intended production model? `(needs human input)` Ticket 20 proposes TLS on the listener. If TLS is added, this property gains a "TLS required before auth data" clause, and the attacker set should include STARTTLS-downgrade attempts.

### Investigation Log

#### Is the shared-password, cleartext listener the intended production model?

- Examined: README:597-607 (`replication_source` and `authentication` sections: one user, one password, `caching_sha2_password` only, no TLS options), `src/binsrv/authentication_config.cpp:25-45`, `src/binsrv/replication_source_config*.hpp` (no TLS fields), `.tickets-snapshot.txt` ticket 20 (Open, High). Its suggested fix includes "Support TLS on the listener", motivated by 9.x replicas' default `SOURCE_SSL=1`.
- Found: the documented model today is a single shared credential over plain TCP. TLS on the listener is a proposed fix in an open ticket, not an accepted plan.
- Not found: maintainer intent or a roadmap decision.
- Conclusion: `(needs human input)`. The property as written is valid for today's code either way.
