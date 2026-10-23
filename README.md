<!--
README.md: porta de entrada do repositório payrouter (v1, fim da Fase B).
Objetivo: quem clona consegue subir o sistema, cadastrar um cliente e criar uma instrução sem
perguntar nada a ninguém. Também diz o que o sistema é, qual a tese, em que estado está e o
que ainda não existe. O README final (arquitetura, medições, limitações) vem na Fase G.
-->

# payrouter

Serviço que recebe instruções de pagamento, roteia cada uma para um participante externo
e trata o **estado desconhecido como estado de primeira classe**: quando o envio dá timeout,
o sistema não sabe se a operação aconteceu, registra essa incerteza e a resolve por
conciliação ativa.

## Tese

Correção quando a transação não existe mais. Não há `ROLLBACK` para uma mensagem já enviada
a outro sistema, então o projeto documenta decisões defensáveis sob incerteza: event
sourcing, outbox transacional, consumidores idempotentes por versão e conciliação por
consulta de status, nunca por reenvio.

Isso já aparece na borda HTTP. Se o prazo de uma requisição estoura no meio do commit, o
servidor não sabe se a instrução foi gravada: responde `504` e o cliente repete com a mesma
`Idempotency-Key`, o que é seguro porque a chave é gravada na mesma transação do evento.

## Estado atual

Fases A e B concluídas (sessões 1 a 8): domínio com a máquina de estados, event store em
Postgres com concorrência otimista, aceitar instrução com idempotência escopada, API HTTP
com autenticação por token temporário, correlation ID, logs estruturados, prazos explícitos,
sondas de saúde e desligamento elegante.

Fase C em andamento (sessões 9 e 10): todo evento é gravado junto com uma linha de outbox, na
mesma transação, e o relay publica o outbox no Kafka (Redpanda) com a chave igual ao
`aggregate_id`. A entrega é at-least-once: se o relay cair entre publicar e marcar, a
mensagem sai de novo, e quem consome é idempotente pela versão.

Ainda não existem consumidor, projeção nem participante simulado: a instrução é aceita, vai
para o tópico e fica em `accepted`. O plano completo está em
[`docs/executionPlan.md`](docs/executionPlan.md).

## Subir e testar em cinco minutos

Requisitos: Docker (com Compose) e `make`. No Windows, use Git Bash ou WSL.

```bash
make up            # cria o .env com um JWT_SECRET aleatório e sobe Postgres, migrations, Redpanda, API e relay
make demo-client   # cadastra o cliente "demo" e imprime o client_secret (uma única vez)
```

Com o `client_secret` em mãos:

```bash
# 1. autenticar: devolve um token que vale 15 minutos
curl -s localhost:8080/auth/token \
  -d '{"client_id":"demo","client_secret":"<segredo>"}'

# 2. criar uma instrução (Idempotency-Key obrigatório, valor sempre em centavos)
curl -si localhost:8080/instructions \
  -H "Authorization: Bearer <access_token>" -H "Idempotency-Key: minha-chave-1" \
  -d '{"debtor":"a","creditor":"b","amount_cents":15000,"end_to_end_id":"e2e-1"}'

# 3. repetir o passo 2 devolve a mesma instrução, com o header Idempotent-Replayed: true

# 4. consultar (estado reconstruído do event store, sem atraso)
curl -s localhost:8080/instructions/<id> -H "Authorization: Bearer <access_token>"
```

Para ver a instrução chegar ao Kafka:

```bash
docker compose exec redpanda rpk topic consume payrouter.instructions.events -n 5
```

Para outro cliente: `make demo-client ID=loja-a`. Para acompanhar os logs: `make logs` (API)
e `make logs-relay` (relay).
Para parar: `make down` (acrescente `-v` ao `docker compose down` para apagar o banco).

## Desenvolvimento

```bash
make db           # só o Postgres, para rodar a API com `go run ./cmd/api`
make migrate-up   # aplica as migrations (o `make up` já faz isso)
make test         # go test -race ./... (os testes de integração usam o Docker)
make test-flake   # os testes de concorrência 20 vezes sob -race
make lint
```

Para rodar a API fora do Docker, exporte `DATABASE_URL`
(`postgres://payrouter:payrouter@localhost:5432/payrouter?sslmode=disable`) e `JWT_SECRET`
(o valor do `.env`) e use `go run ./cmd/api`. Clientes: `go run ./cmd/clientctl create <id>`.

## API

| Rota | Resposta |
|---|---|
| `POST /auth/token` | `200` com `access_token`; `401 invalid_credentials` igual para cliente inexistente, segredo errado e cliente inativo |
| `POST /instructions` | `202` com `Location`; repetição da mesma chave devolve a mesma resposta com `Idempotent-Replayed: true`; a mesma chave com outro corpo é `422` |
| `GET /instructions/{id}` | `200`, `404` se não existir, `400` se o id não for UUID |
| `GET /healthz`, `GET /readyz` | sondas públicas, veja abaixo |

Erros têm sempre o formato `{"error":{"code":"...","message":"..."}}`. Token ausente,
inválido ou expirado dá `401` com `WWW-Authenticate: Bearer`: o cliente autentica de novo.
Um token já emitido vale até expirar mesmo se o cliente for desativado
(`clientctl deactivate`), no máximo 15 minutos.

### Configuração

| Variável | Padrão | Efeito |
|---|---|---|
| `DATABASE_URL` | (obrigatória) | conexão com o Postgres |
| `JWT_SECRET` | (obrigatória) | assina os tokens; no mínimo 32 bytes |
| `ADDR` | `:8080` | endereço de escuta |
| `LOG_FORMAT` | `json` | `text` escreve logs legíveis no terminal |
| `REQUEST_TIMEOUT` | `5s` | prazo de cada requisição, repassado ao Postgres pelo contexto |

### Observabilidade e prazos

- **Correlation ID**: o header `X-Correlation-ID` do cliente é reaproveitado se tiver até 64
  caracteres de `[A-Za-z0-9._-]`; senão o servidor gera um UUID. Ele volta em toda resposta
  (inclusive erros) e aparece em todo log da requisição.
- **Log de acesso**: uma linha por requisição com `method`, `route` (o padrão, como
  `GET /instructions/{id}`), `status`, `duration_ms` e `client_id`. Nunca grava token, corpo
  nem valores.
- **Prazo estourado**: `504 request_timeout`, com o resultado desconhecido. Cliente que
  desiste da conexão vira `499` nos logs.
- **Sondas** (públicas, sem log de acesso): `GET /healthz` diz se o processo está vivo e
  nunca consulta o banco; `GET /readyz` pinga o Postgres (limite de 2 s) e responde `503`,
  sem detalhe no corpo, se ele não responde.
- **Prazos do servidor**: `ReadHeaderTimeout` 5 s, `ReadTimeout` 10 s, `WriteTimeout` 15 s
  (maior que o prazo da requisição, para o 504 caber), `IdleTimeout` 60 s; pool com
  `ConnectTimeout` 5 s e conexões renovadas a cada 30 min.

### Desligamento

Ao receber `SIGINT` ou `SIGTERM` (`docker stop`), a API para de aceitar conexões, espera as
requisições em andamento por até 10 s e só então fecha o pool do Postgres. Uma requisição que
não termina nesse prazo é cortada, e para o cliente isso é o mesmo resultado desconhecido do
`504`: repete com a mesma `Idempotency-Key`. Um segundo sinal derruba o processo na hora.

## Limitações conhecidas (v1)

- A instrução fica em `accepted`: o relay a publica no Kafka, mas ainda não há processor nem
  participante (sessões 11 e 12).
- Entrega at-least-once: o mesmo evento pode aparecer duas vezes no tópico depois da queda do
  relay. Ainda não há consumidor; quando houver, ele descarta o que já viu pela versão.
- O outbox publicado não é apagado: não há rotina de limpeza ainda.
- Token sem refresh e sem revogação antes de expirar; sem rate limit no `/auth/token`.
- Sem TLS: pensado para rodar atrás de um proxy ou só em rede local.
- Segredos de desenvolvimento do Postgres estão no `docker-compose.yml`; não use fora do
  ambiente local.

## Documentação

- [`docs/escope.md`](docs/escope.md): objetivo, escopo, agregado, modelo de dados, decisões e experimentos
- [`docs/executionPlan.md`](docs/executionPlan.md): fases, sessões, riscos e perguntas de defesa
- [`docs/commitpattern.md`](docs/commitpattern.md): padrão das mensagens de commit
