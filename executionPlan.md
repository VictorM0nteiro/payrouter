# payrouter — Plano de execução

> Companheiro do documento de escopo. O escopo define **o que** e **por quê**; este define
> **quando** e **como**. 33 sessões de ~2h30, ou ~21 semanas a 4h por semana.

---

## 1. Como trabalhar com o Claude Code neste projeto

A lição do projeto anterior é concreta e vale repetir aqui porque custou caro: a seção de
análise do relatório de performance do wallet-go foi redigida pelo Claude, e isso virou
uma limitação declarada no README. O texto estava correto; o problema é que a conclusão
que o projeto existia para você defender não era sua.

Neste projeto a regra é a mesma, com uma frente a mais de atenção: **arquitetura
assíncrona é fácil de gerar e difícil de defender.** Um agente escreve um consumidor
idempotente em quatro minutos. Explicar por que a idempotência é por versão e não por
tabela de deduplicação, sob pergunta, é outra coisa.

### Delegue sem culpa

Boilerplate de adapter, DTOs, scaffolding de teste table-driven, `docker-compose.yml`,
Makefile, migrations, configuração de CI e lint, o participante simulado, o harness de
carga, formatação de relatório, rascunho de ADR a partir de decisão que **você** já tomou.

### Não delegue

- **`internal/domain/`** — a máquina de estados e a reconstrução do agregado. É o que a
  entrevista cobra e é a parte mais barata de escrever à mão.
- **A idempotência do consumidor e o controle de concorrência otimista.** São os dois
  mecanismos centrais da tese.
- **A afinidade por chave e o watermark de offset do worker pool.** É onde paralelismo
  vira bug de ordem; o redimensionamento em si pode ser delegado, a invariante não.
- **A decisão** de qualquer ADR. O Claude Code redige; a escolha é sua.
- **A análise das quatro medições.** "Por que o lag cresce assim com 4 consumidores" é a
  pergunta de entrevista inteira.

### A regra de aceitação

Não faça commit de código que você não consiga explicar em dois minutos, em voz alta, sem
olhar. Se não conseguir, peça a explicação antes de aceitar; se ainda não fechar,
reescreva à mão.

### `CLAUDE.md` na raiz

Crie na sessão 1.

```markdown
# payrouter

Roteador de instruções de pagamento. Trata o estado desconhecido (timeout sem resposta)
como estado de primeira classe, resolvido por conciliação ativa.

## Tese
Correção quando a transação não existe mais. Qualquer adição precisa passar no teste:
isso só existe porque a fronteira é assíncrona?

## Regras invioláveis
- `internal/domain/` não importa pgx, net/http, kafka, mongo nem redis.
- Efeito colateral (enviar ao participante) vive no lado de comando, NUNCA na projeção.
  Projeção é função pura dos eventos e tem que ser replayável.
- Append no event store usa versão esperada; o conflito é o UNIQUE (aggregate_id, version).
- Consumidor é idempotente pela versão do agregado, não por tabela de deduplicação.
- Kafka particiona por aggregate_id. Nenhuma ordenação global é imposta.
- GET de um agregado reconstrói do event store (autoritativo).
  GET de lista lê a projeção (eventual). Está no contrato da API.
- Transição ilegal de estado é erro tipado no domínio, não constraint de banco.
- Dinheiro é int64 de centavos. Nunca float, nem em log, nem em JSON.
- Relay publica e DEPOIS marca published_at. At-least-once, assumido.
- No limite de saída, backpressure (pausa a partição), nunca descarte.
- Pools elásticos (`internal/elastic`) nunca executam dois eventos do mesmo aggregate_id
  ao mesmo tempo nem fora de ordem; offset só avança até o watermark contíguo.
  Pool nunca reenvia por conta própria: reduzir limite em timeout é controle de carga.
- 429 do participante = não processado, reenviar é seguro.
  Timeout = não sei, a única operação segura é consultar status. Nunca confunda os dois.

## Comandos
make test        # go test -race ./...
make lint
make up          # docker compose up -d
make migrate-up
make replay      # reconstrói a projeção do zero

## Estado atual
Sessão N — <o que está pronto>
```

Atualize a linha de estado ao fim de cada sessão.

---

## 2. Convenções

**Branches.** Trabalho solo: commits direto na `main`, sem PR. O CI roda no `push`.

**Commits.** Imperativo, uma mudança lógica por commit.

**Testes.** Table-driven como padrão. O nome do caso descreve o comportamento:
`evento_com_versao_menor_e_descartado`.

**Eventos.** Nome no passado, sempre (`InstructionAccepted`, não `AcceptInstruction`).
Comando é imperativo, evento é fato consumado. Confundir os dois é o erro de vocabulário
mais comum em event sourcing e aparece em revisão.

**Nada de `pkg/`.**

---

## 3. Fases

### Fase A — domínio e event store (sessões 1-4)

| Sessão | Entrega |
|---|---|
| 1 | Repo, CI com lint + `-race`, `CLAUDE.md`, migration 001 com as quatro tabelas |
| 2 | Os oito eventos, `Money`, a máquina de estados, reconstrução do agregado, transições ilegais com erro tipado |
| 3 | Adapter Postgres: append com versão esperada, load do agregado, testcontainers |
| 4 | Teste de concorrência otimista: N goroutines gravando a mesma versão, exatamente uma vence |

**Pronto quando:** `go test ./internal/domain/...` cobre a máquina de estados inteira em
menos de 1s sem Docker, e o teste da sessão 4 passa sob `-race` vinte vezes sem flake.

**Armadilha:** a tentação de dar ao agregado um método que grava. O domínio decide e
produz eventos; quem grava é o adapter. Se o domínio importar pgx, a fase A falhou.

---

### Fase B — comando e leitura (sessões 5-8)

| Sessão | Entrega |
|---|---|
| 5 | Caso de uso "aceitar instrução" + idempotência escopada com fingerprint, na mesma transação |
| 6 | HTTP: `POST /instructions` (202), `GET /instructions/{id}` autoritativo, DTOs, erro de domínio → status |
| 7 | Correlation ID, `log/slog`, timeouts explícitos, `context` propagado, `/healthz` e `/readyz` separados |
| 8 | `docker compose up`, graceful shutdown completo, README v1 |

**Pronto quando:** outra pessoa clona, sobe e cria uma instrução sem te perguntar nada.

**Ponto apresentável 1.** Se um processo seletivo abrir aqui, você tem um serviço
event-sourced funcionando e documentado.

---

### Fase C — a fronteira assíncrona (sessões 9-12)

| Sessão | Entrega |
|---|---|
| 9 | Outbox gravado na mesma transação do evento; invariante 2 no CI |
| 10 | `cmd/relay` com `FOR UPDATE SKIP LOCKED` publicando no Kafka; teste matando o relay no meio |
| 11 | `cmd/participant`: os seis comportamentos configuráveis, incluindo responder depois do timeout |
| 12 | `cmd/processor` consumindo `InstructionAccepted`, roteando, e aplicando a resposta |

**Pronto quando:** uma instrução criada via HTTP chega a `settled` ou `rejected` sem
intervenção, passando por Kafka, e o teste de interrupção do relay prova que nada se
perdeu.

**Armadilha:** publicar no Kafka dentro do handler. Isso é *dual write* e é exatamente o
que o outbox existe para evitar — o evento tem que estar na mesma transação SQL.

---

### Fase D — o estado desconhecido (sessões 13-20)

O coração do projeto. Reserve fôlego: é a fase que mais vai render em entrevista e a que
mais vai quebrar.

| Sessão | Entrega |
|---|---|
| 13 | Timeout no envio → `RoutingTimedOut` → `unknown`, com a task de conciliação criada na mesma transação |
| 14 | Loop de conciliação: varredura por `next_attempt_at`, consulta de status, backoff com jitter |
| 15 | `ReconciliationExhausted` após N tentativas; **o teste do caso cruel**: participante responde depois do timeout e a conciliação descobre que a operação aconteceu |
| 16 | Teste ponta a ponta dos seis comportamentos do participante |
| 17 | Rate limit de saída: limitador antes do envio, `pause`/`resume` da partição, `RoutingThrottled` no `429`. Mais o token bucket de entrada portado do wallet-go |
| 18 | `internal/elastic.WorkerPool`: min/max, crescimento por fila e espera, encolhimento com histerese, afinidade por `aggregate_id`, watermark contíguo de offset. Testes unitários sem Kafka, sob `-race` |
| 19 | Conexões elásticas para o participante: limitador AIMD de requisições em voo + `http.Transport` acompanhando o limite; integração do pool no processor |
| 20 | Folga e dívida acumulada |

**Pronto quando:** com o participante configurado para "processa mas não responde", a
instrução passa por `unknown` e termina em `settled` sem nenhuma intervenção manual, e o
teste prova que **nenhuma instrução foi enviada duas vezes** nesse caminho.

**Ponto apresentável 2.** Aqui a tese do projeto está provada. Tudo depois disso é
qualidade de engenharia em cima de uma ideia que já funciona.

**Armadilha:** reenviar a instrução em vez de consultar o status. Sob `unknown`, reenviar
pode cobrar duas vezes — consulta é a única operação segura. Essa distinção é o ADR 5 e é
a melhor resposta curta que o projeto te dá.

---

### Fase E — projeção (sessões 21-25)

| Sessão | Entrega |
|---|---|
| 21 | `cmd/projector`: consumidor Kafka, projeção Mongo, idempotência por `version` |
| 22 | Testes de reentrega do mesmo evento e de evento fora de ordem |
| 23 | Redis: cache de status com versão; `GET /instructions?status=...` pela projeção |
| 24 | Replay completo: comando que descarta a projeção e reconstrói do event store |
| 25 | Folga |

**Pronto quando:** você apaga a coleção do Mongo, roda `make replay`, e a projeção volta
idêntica — sem nenhuma mensagem reenviada ao participante. Esse teste é a prova do ADR 2.

**Armadilha:** o projector precisando de estado próprio além da versão. Se ele precisar
consultar o Postgres para decidir o que fazer com um evento, a projeção deixou de ser
função pura dos eventos.

---

### Fase F — observabilidade e medição (sessões 26-29)

| Sessão | Entrega |
|---|---|
| 26 | OTel: trace atravessando api → relay → Kafka → processor/projector; métricas de lag, tentativas e fila de conciliação |
| 27 | Harness de carga, instrumentação do lag, seed determinístico |
| 28 | Experimento 7.1: lag da projeção com 1, 2 e 4 consumidores, dois níveis de carga |
| 29 | Experimentos 7.2 (replay com 10k/100k/1M) e 7.4 (saturação do participante, pool fixo contra elástico). 7.3 só se sobrar tempo |

**Pronto quando:** `docs/bench/` tem os dados brutos e qualquer pessoa repete a execução a
partir do que está escrito.

**Armadilha:** propagação de contexto através do Kafka não acontece de graça. O
`traceparent` tem que viajar nos headers da mensagem e ser extraído do outro lado, senão
você tem dois traces desconexos em vez de um. É a parte que mais frequentemente fica pela
metade.

---

### Fase G — falha induzida e documentação (sessões 30-33)

| Sessão | Entrega |
|---|---|
| 30 | Roteiros 1-4: Kafka cai e volta, Mongo cai, processor morre no meio do roteamento, `kill -9` entre o append e o publish |
| 31 | Roteiros 5-6 e `docs/resilience-report.md` com as hipóteses escritas antes |
| 32 | `docs/performance-report.md` + os 10 ADRs |
| 33 | README final, limitações conhecidas, folga |

**O protocolo, igual ao do projeto anterior:** cada falha vira uma hipótese escrita
**antes**, o comportamento observado depois, e a divergência preservada no texto.
Hipótese errada não é corrigida retroativamente — é ela que prova que o teste foi real.

O roteiro mais valioso é `kill -9` entre o append do evento e a publicação: é o cenário
que o outbox existe para cobrir, e é onde você demonstra que a reentrega funciona.

---

## 4. Riscos e contingência

| Risco | Sinal | Resposta |
|---|---|---|
| **Máquina não aguenta a stack** (Postgres + Kafka + Mongo + Redis + 5 processos) | Swap, lentidão, número instável | Use **Redpanda** no lugar do Kafka para desenvolvimento e teste: compatível com o protocolo, sobe em ~1s contra ~20s, consome uma fração da memória. Mantenha Kafka real só na execução de medição |
| Testcontainers com Kafka deixa a suíte lenta | Suíte acima de 3min | Container compartilhado por pacote, com tópico novo por teste; ou Redpanda |
| Fase D estoura | Sessão 16 não fecha na semana 11 | Corte o `ReconciliationExhausted` com escalonamento e deixe só o limite de tentativas |
| Pool elástico reordena ou perde mensagem ao redimensionar | Teste de propriedade falha: mesmo agregado fora de ordem, ou offset commitado à frente de mensagem em voo | Fixe o pool em tamanho constante, prove ordem e watermark, e só então ligue a elasticidade. Se não fechar, o experimento 7.4 compara apenas pools fixos e o limite fica declarado |
| `pause`/`resume` de partição dá nó | Consumidor para e não volta, ou duplica | Comece limitando com token bucket antes do envio e só depois pause a partição. O limitador sozinho já evita estourar o participante; o pause é o que evita acumular na memória |
| Medição ruidosa | Variação acima de 15% entre repetições | Mude o ambiente antes de aumentar repetições. A lição do wallet-go: gerador competindo com os serviços produz curva não-monotônica |
| Processo seletivo abre antes da fase F | — | Pare na sessão 17, declare o estado no README, apresente assim. A tese já está provada |
| Escopo inflando | Você está escrevendo saga, ou um segundo agregado | Releia a régua: isso só existe porque a fronteira é assíncrona? Saga não entra |

---

## 5. As onze perguntas de defesa

Uma por semana, respondida em voz alta, cronometrada em dois minutos, com referência ao
seu código. É a atividade que converte o projeto em aprovação.

1. O envio deu timeout. O que o sistema faz, e por que não pode simplesmente reenviar?
2. O mesmo evento chega duas vezes no projector. O que acontece?
3. Dois eventos do mesmo agregado chegam fora de ordem. E agora?
4. Dois comandos concorrentes no mesmo agregado. Quem ganha, e como você sabe?
5. Por que o envio ao participante não pode morar no consumidor de eventos?
6. O cliente cria uma instrução e consulta em seguida. Ele vê?
7. Você apagou a projeção por engano. Qual é o procedimento, e quanto tempo leva?
8. Por que particionar por `aggregate_id` e não garantir ordem global?
9. O participante devolveu `429` e, em outra instrução, deu timeout. Por que você trata as duas de formas opostas?
10. A entrada chega a quatro vezes o que o participante aceita. O que o sistema faz, e o que ele não faz?

11. O worker pool cresce de 2 para 20 goroutines no meio da carga. Como você garante que dois eventos do mesmo agregado não rodam juntos nem fora de ordem, e que nenhum offset é commitado antes da hora?

A pergunta 1 é a que o projeto existe para responder. A 7 é a que você vai responder com
um número medido, o que quase nenhum candidato consegue.

---

## 6. Checklist final

- [ ] `go test -race ./...` verde no CI
- [ ] As duas invariantes do event store testadas automaticamente
- [ ] Teste de concorrência otimista sem flake em 20 execuções
- [ ] Teste de reentrega e de evento fora de ordem no projector
- [ ] O caso cruel coberto: participante processa, não responde, conciliação descobre
- [ ] `make replay` reconstrói a projeção sem reenviar nada ao participante
- [ ] Trace único atravessando os cinco processos
- [ ] `docker compose up` funciona em máquina limpa
- [ ] Sob entrada acima do limite de saída, nada se perde e o consumidor recupera
- [ ] Relatórios de medição com metodologia e ameaças à validade
- [ ] 10 ADRs escritos
- [ ] Relatório de resiliência com pelo menos uma hipótese errada preservada
- [ ] README com limitações conhecidas
- [ ] Teste de propriedade do pool elástico: ordem por agregado e watermark preservados sob redimensionamento, `-race`, 20 execuções
- [ ] Você responde as onze perguntas em voz alta, sem olhar, em dois minutos cada

O último item é o único sem código associado e é o que decide a entrevista.