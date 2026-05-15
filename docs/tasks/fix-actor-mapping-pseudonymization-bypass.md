# Fix: Bypass de pseudonimização em actor_mappings (PUT sobrescreve `[REDACTED]`)

**Origem:** Pentest Taura Security — Relatório Matcher (28/04/2026)
**Criticidade:** Média (Impacto: Médio × Probabilidade: Médio)
**Branch:** `fix/governance-actor-mapping-pseudonymization-bypass`
**Princípio de design:** actor_mapping é registro de auditoria/governança — **append-only para campos de identidade**. Pseudonimização é **irreversível** por `actor_id`.

---

## Contexto da vulnerabilidade

1. `POST /v1/governance/actor-mappings/{ID}` cria mapping com `display_name`/`email` em texto plano.
2. `POST /v1/governance/actor-mappings/{ID}/pseudonymize` substitui ambos por `[REDACTED]`.
3. `PUT /v1/governance/actor-mappings/{ID}` subsequente envia novos `display_name`/`email` e **sobrescreve** o `[REDACTED]` — pseudonimização é revertida silenciosamente.

### Causa raiz

- **Service:** `internal/governance/services/command/actor_mapping_commands.go:95-128` (`UpsertActorMapping`) — não verifica estado existente antes de chamar `repo.Upsert`.
- **SQL:** `internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql.go:78` — `ON CONFLICT (actor_id) DO UPDATE SET display_name = COALESCE(EXCLUDED.display_name, ...)` aceita qualquer valor não-nulo, inclusive sobre `[REDACTED]`.
- **Helper existente, porém não usado em write path:** `entities.ActorMapping.IsRedacted()` em `internal/governance/domain/entities/actor_mapping.go:96-106`.

---

## Comportamento desejado

| Cenário | Comportamento esperado |
|---|---|
| `actor_id` inexistente | Criar com `display_name`/`email`. |
| Existe não-pseudonimizado + PUT idempotente (mesmo valor) | **No-op** (200/204). Preserva semântica HTTP PUT. |
| Existe não-pseudonimizado + PUT com `email` diferente | **409 Conflict** (`ErrActorMappingImmutable`). Cliente deve criar nova entry. |
| Existe não-pseudonimizado + PUT com `display_name` diferente | **409 Conflict**. Auditoria exige imutabilidade. |
| Existe pseudonimizado + PUT com qualquer PII | **409 Conflict**. Pseudonimização é irreversível. |
| `POST /pseudonymize` | Mantém comportamento atual. |
| `DELETE` | Mantém comportamento atual (right-to-erasure). |

**Idempotência:** PUT com payload idêntico ao estado atual NÃO retorna 409 — retorna sucesso (no-op). Apenas mutação **real** dispara 409.

---

## Decisões técnicas

1. **Defense-in-depth obrigatório:** validação no service E no SQL. Read-before-write puro no service é vulnerável a TOCTOU.
2. **Substituir upsert por insert + erro em conflito:**
   - SQL passa a usar `INSERT ... ON CONFLICT (actor_id) DO NOTHING RETURNING ...`
   - Quando `RETURNING` é vazio, repositório faz `SELECT` do registro atual e compara com payload:
     - Iguais → retorna entidade atual (idempotente)
     - Diferentes → retorna `ErrActorMappingImmutable`
   - Toda a sequência (`INSERT ... ON CONFLICT DO NOTHING` + `SELECT` comparativo) precisa rodar dentro da mesma transação com isolamento adequado para evitar TOCTOU.
3. **Renomear `UpsertActorMapping` → `CreateOrGetActorMapping`** no service. Handler HTTP mantém verbo `PUT` por compat externa, mas mapeia para o novo método.
4. **Novos erros de domínio** em `internal/governance/services/command/commands.go` (ou arquivo dedicado):
   - `ErrActorMappingImmutable` (mutação tentada)
   - `ErrActorMappingPseudonymized` (subtipo opcional para clareza — pode ser apenas msg diferente do mesmo sentinel)
5. **Mapeamento HTTP:** handler converte `ErrActorMappingImmutable` em **HTTP 409** com payload de erro explicativo.

---

## Acceptance criteria

### AC1 — Criação continua funcionando
- `PUT /v1/governance/actor-mappings/{id}` em ID inexistente cria mapping. Status 201/200. ✅
- `display_name` e `email` retornados em texto plano (sem redação). ✅

### AC2 — PUT idempotente é no-op
- `PUT` em mapping existente com `display_name`/`email` **idênticos** retorna sucesso (200/204), sem alterar `updated_at` ou disparar conflito.
- Teste E2E + integração cobrindo este caminho.

### AC3 — PUT tenta mudar `email`
- `PUT` em mapping existente com `email` diferente retorna **409 Conflict**.
- Body do erro inclui mensagem explicativa orientando criação de novo `actor_id`.
- Dados armazenados permanecem inalterados.

### AC4 — PUT tenta mudar `display_name`
- `PUT` em mapping existente com `display_name` diferente retorna **409 Conflict**.
- Dados armazenados permanecem inalterados.

### AC5 — PUT após pseudonimização (cenário do pentest)
- `PUT` em mapping com `display_name = "[REDACTED]"` e `email = "[REDACTED]"` recebendo qualquer PII em texto plano retorna **409 Conflict**.
- Dados armazenados permanecem `[REDACTED]`.
- Reproduz exatamente o ataque do PoC do relatório.

### AC6 — Pseudonimização continua funcionando
- `POST /v1/governance/actor-mappings/{id}/pseudonymize` em mapping existente continua redigindo ambos os campos.
- Comportamento atual preservado.

### AC7 — DELETE após pseudonimização
- `DELETE /v1/governance/actor-mappings/{id}` em mapping pseudonimizado funciona normalmente.
- Garantia de right-to-erasure (LGPD/GDPR).

### AC8 — Concorrência (TOCTOU)
- Duas requisições simultâneas tentando atualizar o mesmo `actor_id` com payloads diferentes: pelo menos uma falha com 409 ou ambas resolvem corretamente (uma cria, outra recebe conflito).
- Nenhuma janela onde `[REDACTED]` pode ser sobrescrito.

### AC9 — Telemetria
- Tentativas de mutação geram span com `HandleSpanBusinessErrorEvent` (não `HandleSpanError`, pois é erro de cliente).
- Métrica/log estruturado registra `actor_id_prefix` (não o ID completo) para auditoria de tentativas de bypass.

### AC10 — OpenAPI
- Spec do endpoint `PUT /v1/governance/actor-mappings/{actorId}` documenta o 409 com schema de erro.
- `make generate-docs` atualiza `docs/swagger/`.

---

## Arquivos afetados (atualizado pós-implementação)

> Atualizado em 2026-05-15 para refletir paths/nomes reais da implementação. Os paths originais especulados durante o planejamento foram substituídos pelos paths efetivos do código entregue.

### Implementação
- `internal/governance/services/command/actor_mapping_commands.go` — método `CreateOrGetActorMapping` + wrapper `UpsertActorMapping` retrocompatível, guard via `errors.Is(err, ErrActorMappingImmutable)`, span business event + SafeError logging.
- `internal/governance/domain/errors/errors.go` — sentinel `ErrActorMappingImmutable` (source of truth, parallel to `ErrActorMappingNotFound`/`ErrAuditLogNotFound`).
- `internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql.go` — SQL `INSERT ... ON CONFLICT DO NOTHING RETURNING` + transactional `SELECT` compare via `actorMappingPIIDiffers` / `stringPtrEqual`.
- `internal/governance/adapters/postgres/actor_mapping/errors.go` — type-alias `var ErrActorMappingImmutable = governanceErrors.ErrActorMappingImmutable` para uso cross-layer com `errors.Is`.
- `internal/governance/domain/repositories/actor_mapping_repository.go` — docstring do contrato `Upsert` atualizada para append-only.
- `internal/governance/domain/entities/actor_mapping.go` — docstring do struct atualizada com o contrato de imutabilidade (não foi adicionado `IdentityEquals` helper; a comparação ficou no adapter via `actorMappingPIIDiffers`).
- `internal/governance/adapters/http/handlers_actor_mapping.go` — `writeConflict` mapeia `ErrActorMappingImmutable` → 409 com slug `governance_actor_mapping_immutable` (`MTCH-0604`).
- `internal/shared/adapters/http/error_catalog.go` — registro do `defGovernanceActorMappingImmutable` + slug map.
- `internal/shared/adapters/http/handler_helpers.go` — novo helper `LogSpanBusinessEvent` (span business event + SafeError).
- `pkg/constant/errors.go` — constante `CodeGovernanceActorMappingImmutable = "MTCH-0604"`.
- `migrations/000033_actor_mapping_immutable_comment.{up,down}.sql` — `COMMENT ON TABLE` documentando o contrato append-only (documentação apenas; o enforcement está no application code).

### Testes
- `internal/governance/services/command/actor_mapping_immutable_test.go` — unit test com mock repo (gomock), cobertura de AC1-AC5.
- `internal/governance/services/command/actor_mapping_immutability_property_test.go` — property test (rapid) com oráculo independente (commit 9a2dd570) cobrindo invariantes de irreversibilidade, idempotência e rejeição de mutação.
- `internal/governance/adapters/postgres/actor_mapping/actor_mapping_immutable_sqlmock_test.go` — sqlmock unit cobrindo AC1-AC5 em camada SQL.
- `internal/governance/adapters/postgres/actor_mapping/actor_mapping_immutability_fuzz_test.go` — fuzz dos helpers `stringPtrEqual` e `actorMappingPIIDiffers` (reflexividade, simetria, semântica nil/empty).
- `internal/governance/adapters/postgres/actor_mapping/actor_mapping_immutability_integration_test.go` — integration test com testcontainers cobrindo AC1-AC5, AC7, AC8 (12 goroutines concorrentes por cenário).
- `internal/governance/adapters/http/handlers_actor_mapping_immutable_test.go` — handler test cobrindo AC1-AC5 com status codes HTTP.
- `internal/governance/domain/entities/actor_mapping_fuzz_test.go` — fuzz do constructor `NewActorMapping` (trimming, length, UTF-8, NUL bytes).
- `internal/governance/domain/entities/actor_mapping_property_test.go` — property test do `IsRedacted` (biconditional, nil receiver, partial redaction).
- `tests/chaos/actor_mapping_chaos_test.go` — chaos suite com Toxiproxy (connection drop, latency, partition + heal).
- `tests/chaos/harness.go` + `tests/chaos/common.go` — `LockHarnessForTest` + `testLockHeld` atomic.Bool safeguard para serialização do chaos suite.
- `tests/integration/governance/actor_mapping_test.go` — cross-layer integration (partial-payload PUT rejection).
- `tests/e2e/journeys/actor_mapping_test.go` — e2e journey (`TestActorMapping_IdempotentSamePayload`, `TestActorMapping_MutationReturnsConflict`).
- `internal/shared/adapters/http/handler_helpers_test.go` — unit test do novo `LogSpanBusinessEvent`.

### Documentação
- `docs/swagger/{docs.go, swagger.json, swagger.yaml}` — regenerados via `make generate-docs`.
- Annotations Swagger no handler: `@Failure 409 {object} sharedhttp.ErrorResponse "Actor mapping identity is immutable (MTCH-0604)"`.
- `migrations/000033_actor_mapping_immutable_comment.up.sql` — `COMMENT ON TABLE` descrevendo o contrato.

---

## Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Clientes existentes dependem do upsert mutável | Buscar uso interno antes de PR. Documentar mudança como breaking change no CHANGELOG. Comunicar via release notes. |
| TOCTOU em service-only check | Defense-in-depth via SQL — INSERT ON CONFLICT DO NOTHING garante atomicidade. |
| PUT idempotente quebrar retries | AC2 cobre exatamente esse caso — payload idêntico = no-op silencioso. |
| Pseudonymize concorrente com PUT | Pseudonymize já usa transação (`PseudonymizeWithTx`). Verificar que SET TRANSACTION ISOLATION ou row-level locks (`SELECT ... FOR UPDATE`) blindam a ordenação. |

---

## Notas finais

- **Quem revisa:** review obrigatório com foco em segurança. Sugerir `ring:codereview` cobrindo `security-reviewer` + `business-logic-reviewer` no Gate 8.
- **PR description:** referenciar relatório Taura Security 28/04/2026, finding "Remoção de pseudonimização em atualizações cadastrais".
- **Não vazar dados em logs:** continuar usando `entities.SafeActorIDPrefix(actorID)` ao registrar tentativas de bypass.
