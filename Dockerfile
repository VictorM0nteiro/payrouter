# Dockerfile
# Imagem da API, do relay, do participante simulado e do clientctl do payrouter, em dois estágios.
# Objetivo: `docker compose up` conseguir subir a API, o relay e o participante sem Go instalado
# na máquina. O primeiro estágio compila os binários estáticos; o segundo é um Alpine mínimo,
# rodando como usuário sem privilégios, só com eles. O ENTRYPOINT padrão é a API; o compose
# troca por `relay` ou `participant` nos outros serviços. O clientctl serve ao `make demo-client`.

FROM golang:1.25-alpine AS build
WORKDIR /src

# Dependências antes do código: a camada só é refeita quando go.mod/go.sum mudam.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/relay ./cmd/relay \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/participant ./cmd/participant \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/clientctl ./cmd/clientctl

FROM alpine:3.21
RUN adduser -D -u 10001 payrouter
COPY --from=build /out/api /out/relay /out/participant /out/clientctl /usr/local/bin/
USER payrouter
EXPOSE 8080
ENTRYPOINT ["api"]
