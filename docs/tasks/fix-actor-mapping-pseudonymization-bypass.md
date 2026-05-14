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

## Arquivos afetados

### Implementação
- `internal/governance/services/command/actor_mapping_commands.go` — renomear método, adicionar guard, mapear erros.
- `internal/governance/services/command/commands.go` — sentinel `ErrActorMappingImmutable`.
- `internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql.go` — SQL `INSERT ... ON CONFLICT DO NOTHING` + comparação.
- `internal/governance/adapters/postgres/actor_mapping/errors.go` — erro de conflito do adapter, se necessário.
- `internal/governance/domain/repositories/actor_mapping_repository.go` — atualizar contrato da interface.
- `internal/governance/domain/entities/actor_mapping.go` — possível helper `IdentityEquals(other *ActorMapping) bool`.
- `internal/governance/adapters/http/handlers_actor_mapping.go` (ou equivalente) — mapear `ErrActorMappingImmutable` → 409.

### Testes
- `internal/governance/services/command/actor_mapping_commands_test.go` — AC1, AC2, AC3, AC4, AC5, AC6 com mock repo.
- `internal/governance/adapters/postgres/actor_mapping/actor_mapping_sqlmock_test.go` — AC2, AC3, AC4, AC5 em camada SQL.
- `internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql_test.go` (integration) — AC8 com testcontainers + goroutines concorrentes.
- `internal/governance/adapters/http/handlers_actor_mapping_test.go` — AC1, AC2, AC3, AC5, AC7 verificando códigos HTTP.
- Regressão E2E (opcional, se houver suite cobrindo governance flows).

### Documentação
- `docs/swagger/swagger.json` + `swagger.yaml` — via `make generate-docs`.
- Atualizar comentários Swagger no handler com `@Failure 409`.

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
