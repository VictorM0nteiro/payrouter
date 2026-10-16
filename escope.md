# payrouter — Roteador de instruções de pagamento com estado desconhecido

> Documento de escopo. Companheiro do plano de execução. Nome de trabalho; troque se
> quiser.

---

## 1. Objetivo

### O que o sistema é

Um serviço que recebe instruções de pagamento, roteia cada uma para um participante
externo e trata **o estado desconhecido como estado de primeira classe**: quando o envio
dá timeout, o sistema não sabe se a operação aconteceu, registra essa incerteza e a
resolve ativamente por conciliação.

Essa frase vai no topo do README. Tudo que não a sustenta está fora do escopo.

### A tese, e por que ela é diferente da do projeto anterior

O [wallet-go](https://github.com/VictorM0nteiro/wallet-go-) prova correção **dentro** de
uma transação: lock, invariante de soma zero, rollback, tudo ou nada. É a razão pela qual
ele é forte, e é também o seu limite declarado.

Este projeto prova correção **quando a transação não existe mais**. Não há `ROLLBACK` que
desfaça uma mensagem enviada para outro sistema; não há `COMMIT` que torne dois
consumidores consistentes no mesmo instante. O que sobra são decisões defensáveis sobre
incerteza, e é isso que o projeto documenta.

Se a tese fosse a mesma do wallet-go, o segundo projeto seria redundante. A régua para
qualquer adição aqui: **isso só existe porque a fronteira é assíncrona?** Se a resposta
for não, provavelmente é repetição.

### Event sourcing em cinco linhas

O estado de uma instrução não é uma coluna que se atualiza. É a sequência de eventos que
aconteceram com ela, gravados em ordem e nunca editados. O estado atual é obtido
*reconstruindo*: parte do zero e aplica cada evento na ordem.

Três consequências que o projeto explora:

1. **O histórico é a informação, não um subproduto.** "Esta instrução foi tentada três
   vezes, deu timeout nas duas primeiras e o participante confirmou na consulta de status"
   é uma pergunta que um campo `status` não responde e uma sequência de eventos responde
   sem esforço.
2. **Projeções são descartáveis.** Qualquer visão de leitura é função pura dos eventos, e
   portanto pode ser jogada fora e reconstruída. Isso é o que torna um bug de projeção
   recuperável em vez de permanente.
3. **Efeito colateral não pode morar na projeção.** Se o envio da mensagem fosse feito ao
   processar um evento, uma reconstrução reenviaria tudo. É a decisão que organiza a
   arquitetura inteira (ADR 2).

### O problema do estado desconhecido

Você envia a instrução ao participante e o socket expira. Do seu lado, dois mundos são
indistinguíveis:

- a mensagem não chegou, e nada aconteceu
- a mensagem chegou, foi processada, e só a resposta se perdeu

Não existe informação local que separe os dois. Responder "falhou" ao cliente pode ser
mentira — e, num sistema de pagamento, é a mentira cara. Responder "deu certo" também.

A única saída honesta é um terceiro estado: `unknown`. E o que define a qualidade do
sistema é o que ele faz com esse estado, porque `unknown` **não se resolve sozinho**.

### O que o projeto prova

Que você sabe desenhar um agregado event-sourced com concorrência otimista; manter um
consumidor correto sob entrega at-least-once e reordenação; separar efeito colateral de
projeção para que replay seja seguro; resolver incerteza por conciliação em vez de
chutar; e medir o custo da consistência eventual em vez de afirmá-lo.

---

## 2. Escopo

### Dentro

- Recebimento e validação de instruções de pagamento, com `Idempotency-Key`
- Event store em Postgres com concorrência otimista por versão de agregado
- Outbox transacional e relay publicando no Kafka
- Roteamento assíncrono para um participante externo simulado
- Tratamento de timeout: estado `unknown` e conciliação ativa com backoff
- Projeção de documento em MongoDB, com consumidor idempotente
- Cache de status em Redis
- Replay completo de projeção
- Observabilidade com OpenTelemetry e as três medições da seção 7
- Participante simulado com comportamento configurável, incluindo mentir
- Rate limit **de saída** para o participante, com backpressure no consumidor
- Rate limit de entrada por cliente (portado do wallet-go, declarado como reaproveitamento)

### Fora (deliberadamente)

- Ledger, saldo, partidas dobradas — isso é o wallet-go, e repetir desperdiça o projeto
- Integração com SPI, Bacen, Dict ou qualquer sistema real
- Autenticação além de uma API key estática
- Multi-moeda, agendamento, instrução recorrente
- Frontend de qualquer tipo
- Saga com compensação entre múltiplos agregados
- Kubernetes, service mesh, nuvem (fila de extras)

**Regra de escopo:** diante de "adiciono uma feature ou aprofundo o que existe",
aprofunde. Um fluxo medido e documentado vale mais que cinco endpoints.

---

## 3. O agregado

Uma `PaymentInstruction`. Toda a complexidade do projeto cabe no ciclo de vida dela.

### Máquina de estados

```
         accepted
             │ roteia
             ▼
         in_flight ──────┬──────────┐
             │           │          │
       resposta ok   rejeitada   timeout
             │           │          │
             ▼           ▼          ▼
          settled    rejected    unknown
                                    │
                         consulta de status
                       ┌────────────┼────────────┐
                       ▼            ▼            ▼
                   settled      rejected      unknown
                                           (reagenda; após N
                                            tentativas, failed
                                            com intervenção
                                            manual registrada)
```

`settled`, `rejected` e `failed` são terminais. `unknown` é o único estado que exige
trabalho de fundo para sair de si mesmo.

Há um quarto caminho de saída de `in_flight` que o diagrama acima omite para não poluir:
o participante responder `429`. Isso **volta** a instrução para `accepted`, aguardando
nova tentativa, e é o oposto do timeout — `429` significa definitivamente não processado,
então reenviar é seguro. No caminho normal isso não deveria acontecer, porque o rate limit
de saída se autolimita antes; quando acontece, significa que o limite está mal
configurado, e o evento registra isso.

### Eventos

| Evento | Significado | Transição |
|---|---|---|
| `InstructionAccepted` | Validada e persistida, nada enviado | → `accepted` |
| `RoutingAttempted` | Mensagem despachada ao participante | → `in_flight` |
| `ParticipantAccepted` | Participante confirmou | → `settled` |
| `ParticipantRejected` | Participante negou, com código | → `rejected` |
| `RoutingTimedOut` | Sem resposta no prazo | → `unknown` |
| `StatusQueried` | Consulta de conciliação feita, com o resultado | permanece ou resolve |
| `RoutingThrottled` | Participante devolveu `429`, nada foi processado | → `accepted` |
| `ReconciliationExhausted` | N consultas sem conclusão | → `failed` |

Oito eventos. Transição ilegal é rejeitada **no domínio**, com erro tipado — não por
constraint de banco, não por `if` espalhado no handler.

---

## 4. Modelo de dados

```sql
-- O event store. Fonte única da verdade.
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    UUID    NOT NULL,
    aggregate_type  TEXT    NOT NULL,
    version         INT     NOT NULL CHECK (version > 0),
    event_type      TEXT    NOT NULL,
    payload         JSONB   NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, version)
);

CREATE INDEX events_aggregate_idx ON events (aggregate_id, version);

-- Outbox transacional: gravado na MESMA transação do evento.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      BIGINT NOT NULL REFERENCES events(id),
    aggregate_id  UUID   NOT NULL,
    topic         TEXT   NOT NULL,
    payload       JSONB  NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;

-- Fila de trabalho do process manager.
CREATE TABLE reconciliation_tasks (
    aggregate_id     UUID PRIMARY KEY,
    attempts         INT  NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX reconciliation_due_idx ON reconciliation_tasks (next_attempt_at);

-- Idempotência do lado de comando. Mesmo contrato do wallet-go.
CREATE TABLE idempotency_keys (
    scope                TEXT NOT NULL,
    endpoint             TEXT NOT NULL,
    key                  TEXT NOT NULL,
    request_fingerprint  TEXT NOT NULL,
    state                TEXT NOT NULL,
    status_code          INT,
    response_body        JSONB,
    aggregate_id         UUID,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, endpoint, key)
);
```

### Invariantes testadas no CI

```sql
-- 1. Versões de um agregado são contíguas a partir de 1, sem buraco.
SELECT aggregate_id
FROM events
GROUP BY aggregate_id
HAVING COUNT(*) <> MAX(version) OR MIN(version) <> 1;

-- 2. Todo evento tem exatamente uma linha de outbox.
SELECT e.id FROM events e
LEFT JOIN outbox o ON o.event_id = e.id
WHERE o.id IS NULL;
```

Ambas devem retornar zero linhas, sempre.

### Projeção em MongoDB

Documento por instrução. A linha do tempo aninhada é o que justifica documento em vez de
tabela: ela é lida junto com o resto, nunca consultada em separado.

```json
{
  "_id": "7c9e...",
  "version": 5,
  "status": "settled",
  "amount_cents": 15000,
  "debtor": "...",
  "creditor": "...",
  "end_to_end_id": "...",
  "attempts": 3,
  "timeline": [
    { "event": "InstructionAccepted", "version": 1, "at": "..." },
    { "event": "RoutingAttempted",    "version": 2, "at": "..." },
    { "event": "RoutingTimedOut",     "version": 3, "at": "..." },
    { "event": "StatusQueried",       "version": 4, "at": "...", "result": "accepted" },
    { "event": "ParticipantAccepted", "version": 5, "at": "..." }
  ],
  "updated_at": "..."
}
```

`version` no documento é o mecanismo de idempotência do consumidor (ADR 3).

### Cache em Redis

`status:<aggregate_id>` → `{"status": "...", "version": N}`, com TTL. A versão viaja no
valor para que o cliente consiga detectar defasagem em vez de confiar cegamente.

---

## 5. Decisões já travadas

**1. Concorrência otimista por versão esperada.** Para gravar a versão 4, o comando
afirma ter lido a versão 3. Se outro escreveu nesse meio, `UNIQUE (aggregate_id,
version)` recusa e o comando relê e tenta de novo. Sem lock de linha, sem
`SERIALIZABLE`.

Isso fecha uma trilogia com o projeto anterior: lock pessimista, isolamento serializável
e versão otimista são três respostas ao mesmo problema, e você sabe dizer quando cada uma
cabe.

**2. Efeito colateral vive no lado de comando, nunca na projeção.** Enviar ao participante
é efeito externo. Projeções são função pura dos eventos e podem ser reconstruídas; o envio
é reação a comando, não a replay.

**3. Idempotência do consumidor é a versão, não uma tabela de deduplicação.** A projeção
guarda `version` e descarta evento com versão menor ou igual. Custa um campo, cobre
reentrega e reordenação com o mesmo mecanismo.

**4. Partição do Kafka por `aggregate_id`.** Ordem garantida dentro de uma instrução, que
é onde ela importa. Entre instruções diferentes não importa, e nenhuma ordenação global é
imposta — dizer isso é mais forte que impor.

**5. Leitura de um agregado é autoritativa; leitura de lista é eventual.**
`GET /instructions/{id}` reconstrói do event store, o que para um agregado é barato.
`GET /instructions?status=unknown` lê a projeção. Isso dissolve read-your-writes sem
inventar máquina nenhuma, e fica explícito no contrato da API.

**6. Entrega at-least-once, assumida.** O relay publica e só então marca `published_at`.
Morrer no meio republica; a ordem inversa perderia evento. A consequência está no item 3.

**7. O participante simulado é parte do sistema, não um mock.** Processo próprio, com
comportamento configurável por variável de ambiente: aceita, rejeita, demora, dá timeout,
**responde depois do timeout** (o caso cruel: processou, mas você não soube) e cai. É o
que torna a tese testável.

**8. Dinheiro em `int64` de centavos.** Mesma regra do projeto anterior: nunca float, em
nenhuma camada, nem em log, nem em JSON.

**9. Rate limit de saída com backpressure, não descarte.** O participante tem um limite
de TPS configurável. Quando a entrada excede esse limite, o processor **desacelera o
consumo** — pausa a partição e não avança o offset — em vez de descartar mensagem ou
acumular na memória. A consequência é que o lag cresce, e isso é correto: a instrução não
se perde, ela espera.

`429` e timeout exigem reações opostas, e essa é a distinção mais importante do projeto
depois do próprio `unknown`: `429` é "definitivamente não processado", logo reenviar é
seguro; timeout é "não sei", logo a única operação segura é consultar status. Duas falhas
que parecem iguais no log e que não podem ser tratadas do mesmo jeito.

O rate limit **de entrada** por cliente é o token bucket do wallet-go, portado. Fica
declarado como reaproveitamento no README, porque não é contribuição deste projeto.

---

## 6. Arquitetura

Hexagonal, na versão mínima, pelo mesmo motivo prático de sempre: o domínio precisa ser
testável sem Kafka, sem Mongo e sem Docker.

```
  POST /instructions
  GET  /instructions/{id}        ┌──────────────┐
  GET  /instructions?...    ───▶ │     api      │  comando + leitura autoritativa
                                 └──────┬───────┘
                                        │ uma transação SQL:
                                        │ events + outbox
                                        ▼
                                 ┌──────────────┐
                                 │  PostgreSQL  │  event store · outbox · tasks
                                 └──────┬───────┘
                                        │
                                 ┌──────▼───────┐
                                 │    relay     │  FOR UPDATE SKIP LOCKED
                                 └──────┬───────┘
                                        ▼
                                   ┌─────────┐
                                   │  Kafka  │  partição por aggregate_id
                                   └────┬────┘
                        ┌───────────────┴───────────────┐
                        ▼                               ▼
                 ┌──────────────┐               ┌──────────────┐
                 │  processor   │               │  projector   │
                 │ roteia +     │               │ Mongo +      │
                 │ concilia     │               │ Redis        │
                 └──────┬───────┘               └──────────────┘
                        │ HTTP
                        ▼
                 ┌──────────────┐
                 │ participant  │  simulado, configurável, pode mentir
                 └──────────────┘
```

```
cmd/
  api/            HTTP: comando e leitura autoritativa
  relay/          outbox → Kafka
  processor/      process manager: roteamento + conciliação (efeito colateral)
  projector/      Kafka → Mongo + Redis (função pura, replayável)
  participant/    participante simulado
  loadtest/       harness de carga e medição
internal/
  domain/         PaymentInstruction, eventos, máquina de estados, Money
                  — zero import de pgx, net/http, kafka, mongo
  app/            casos de uso + portas
  adapters/
    http/         handlers, DTOs, erro de domínio → status
    postgres/     event store, outbox, tasks, idempotência
    kafka/        producer, consumer
    mongo/        projeção
    redis/        cache
    otel/         instrumentação
```

**Teste de que a hexagonal não é decoração:** `go test ./internal/domain/...` roda em
milissegundos e cobre a máquina de estados inteira, incluindo as transições ilegais, sem
nenhum container.

---

## 7. Os experimentos

Quatro medições, todas saindo de infraestrutura que o projeto constrói de qualquer forma.

### 7.1 Lag da projeção

**A pergunta:** quanto tempo entre o commit do evento e a projeção refletir, sob carga, e
como isso escala com o número de consumidores?

| Condição | Variável |
|---|---|
| Consumidores | 1, 2, 4 |
| Carga | taxa sustentada do harness, dois níveis |

Registrar p50/p95/p99 do lag, throughput de eventos processados, e o ponto em que o
consumidor deixa de acompanhar a produção.

### 7.2 Tempo de replay

**A pergunta:** quanto custa reconstruir a projeção do zero, em função do número de
eventos?

Medir com 10k, 100k e 1M eventos. O resultado é a resposta honesta para "e se a projeção
corromper?" — e é também a camada batch da seção 11.

### 7.3 Leitura em cache contra reconstrução

**A pergunta:** qual a latência de `GET /instructions/{id}` reconstruindo do event store,
e quanto o cache de status economiza?

Registrar latência das duas vias e taxa de acerto do cache. Essa é a menos interessante
das quatro e a primeira a cortar se o tempo apertar.

### 7.4 Saturação do participante

**A pergunta:** quando a taxa de entrada excede o limite de saída, o sistema degrada de
forma graciosa ou colapsa?

| Condição | Variável |
|---|---|
| Entrada | 50/s (no limite), 100/s, 200/s |
| Limite de saída | fixo em 50/s |

Registrar: crescimento da fila de conciliação, lag da projeção, latência do `POST`
(precisa continuar respondendo rápido — ele só grava evento), e se o consumidor recupera
quando a entrada volta ao normal.

O resultado esperado é que o lag cresça de forma proporcional e nada se perca. Se alguma
coisa se perder, o backpressure está errado, e descobrir isso medindo é melhor que
descobrir numa pergunta de entrevista.

**Metodologia, para as três:** hardware, SO, versões, parâmetros, semente e comando exato
registrados. Reprodutibilidade é metade da credibilidade; a outra metade é explicar *por
que* o número é o que é.

E a lição do projeto anterior, já aprendida: gerador de carga, serviços e bancos
competindo pela mesma máquina produzem ruído que pode exceder o efeito medido. Declare
isso em "ameaças à validade" em vez de esconder.

---

## 8. ADRs a escrever

Arquivos curtos em `docs/adr/`: contexto, opções consideradas, decisão, consequências.

| # | Decisão | O que defender |
|---|---|---|
| 1 | Event sourcing em vez de coluna de status | O histórico de tentativas é a informação. **Custo assumido:** leitura de um agregado vira reconstrução |
| 2 | Efeito colateral fora da projeção | Sem isso, replay reenvia mensagem. É o que torna projeção descartável |
| 3 | Idempotência do consumidor por versão | Cobre reentrega e reordenação com um campo, em vez de tabela de deduplicação |
| 4 | Concorrência otimista por versão esperada | Comparar com lock pessimista e `SERIALIZABLE` do projeto anterior: quando cada um cabe |
| 5 | `unknown` como estado de primeira classe | Timeout não é falha. Responder "falhou" sem saber é a mentira cara num sistema de pagamento |
| 6 | Partição por `aggregate_id`, sem ordem global | Ordem onde importa, e nada além. Por que ordenação global seria custo sem benefício |
| 7 | Leitura de agregado autoritativa, lista eventual | Dissolve read-your-writes sem máquina extra; o contrato fica honesto |
| 8 | At-least-once em vez de at-most-once | Publicar antes de marcar. A alternativa perde evento, que é pior que duplicar |
| 9 | Backpressure em vez de descarte no limite de saída | Instrução de pagamento não se descarta: ela espera. E por que `429` e timeout, que parecem a mesma falha, exigem reações opostas |

ADR 5 é o mais valioso. É o único que não tem resposta limpa, e é por isso que ele mostra
julgamento em vez de conhecimento.

---

## 9. Stack

```
Linguagem   Go 1.25+
HTTP        net/http + chi
Event store PostgreSQL 16 · pgx/v5 · golang-migrate
Mensageria  Kafka · franz-go (ou segmentio/kafka-go)
Projeção    MongoDB · mongo-driver
Cache       Redis · go-redis
Testes      testify · testcontainers-go · go test -race
Observab.   OpenTelemetry · Prometheus · Grafana · log/slog
Carga       harness próprio em Go
Perfil      go test -bench -benchmem · pprof
Infra       Docker · Docker Compose
CI          GitHub Actions · golangci-lint
```

Biblioteca padrão sempre que a diferença for pequena. Sobre observabilidade: OTel é neutro
de fornecedor e exporta para Datadog e New Relic — você pode dizer isso com verdade sem
pagar nada.

---

## 10. Estrutura do README final

1. Uma frase dizendo o que o sistema faz
2. A tese, em três linhas: o que muda quando a transação acaba
3. Diagrama de arquitetura e os cinco processos
4. A máquina de estados, com destaque no `unknown`
5. Primer de event sourcing em cinco linhas
6. Decisões de design, com link para cada ADR
7. Resultados de medição: as quatro tabelas e a metodologia
8. Relatório de falha induzida: hipótese, observado, divergência
9. Como rodar: `docker compose up` e os testes passando
10. **Limitações conhecidas**

O item 10 é o que mais impressiona, e aqui ele tem material de sobra: sem saga, sem
multi-região, sem ordenação global, participante simulado e não real, conciliação sem
escalonamento humano de verdade.

---

## 11. Fila de extras

Só depois de tudo acima, nesta ordem:

1. **Camada batch na forma da Lambda Architecture** — reconstrução agendada por replay
   completo como caminho autoritativo de correção, convivendo com a projeção incremental.
   O replay já existe por conta do experimento 7.2; o extra é agendá-lo, comparar o
   resultado das duas camadas e registrar a divergência quando houver. **Descrição
   honesta:** "camada de reconstrução por replay mais camada incremental, na forma da
   Lambda Architecture", mais a explicação de por que a Kappa a substituiu na prática
2. **DLQ e envenenamento de mensagem** — evento que o consumidor não consegue processar
   após N tentativas vai para tópico morto, com teste
3. **Deploy em host único** com Terraform e Caddy
4. **AWS gerenciado** — RDS, MSK ou Kafka em EC2, ElastiCache
5. **Elasticsearch como terceira projeção** — busca por instrução; fecha o item que ficou
   de fora da seleção

---

## 12. Como começar

A primeira sessão produz, nesta ordem:

1. Repositório, estrutura de pastas, CI com lint e teste rodando desde o primeiro commit
2. `CLAUDE.md` com as regras invioláveis
3. Migration 001 com `events`, `outbox`, `reconciliation_tasks` e `idempotency_keys`
4. A máquina de estados no domínio, com os sete eventos e seus testes — **sem banco, sem
   Kafka, sem nada**

Comece pelo domínio. A máquina de estados é a única parte do sistema que você precisa
saber de cor para defender o projeto falando, e é a mais barata de construir.