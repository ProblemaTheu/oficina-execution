package mensageria

import (
	"context"
	"net/http"
	"strings"

	"github.com/newrelic/go-agent/v3/newrelic"
)

// Tracer propaga o trace distribuído através da fila (backlog F4-7.1).
//
// Os headers são capturados na GRAVAÇÃO da outbox (Capturar), não na
// publicação: quando o relay roda, a transação HTTP ou de consumo que gerou a
// mensagem já terminou.
type Tracer interface {
	// Capturar devolve os headers de trace da transação corrente (vazio se não houver).
	Capturar(ctx context.Context) map[string]string
	// IniciarConsumo abre a transação de consumo continuando o trace recebido.
	// O fim deve ser chamado ao terminar o processamento.
	IniciarConsumo(ctx context.Context, nome string, headers map[string]string) (context.Context, func(err error))
}

// TracerNulo não propaga nada. Usado em testes e quando o New Relic está desligado.
type TracerNulo struct{}

func (TracerNulo) Capturar(context.Context) map[string]string { return map[string]string{} }
func (TracerNulo) IniciarConsumo(ctx context.Context, _ string, _ map[string]string) (context.Context, func(error)) {
	return ctx, func(error) {}
}

// TracerNewRelic propaga o trace com os headers do agente New Relic
// (newrelic, traceparent, tracestate).
type TracerNewRelic struct {
	App *newrelic.Application
}

// Capturar insere os headers de DT da transação do contexto.
func (TracerNewRelic) Capturar(ctx context.Context) map[string]string {
	out := map[string]string{}
	txn := newrelic.FromContext(ctx)
	if txn == nil {
		return out
	}
	h := http.Header{}
	txn.InsertDistributedTraceHeaders(h)
	for k := range h {
		out[strings.ToLower(k)] = h.Get(k)
	}
	return out
}

// IniciarConsumo abre uma transação "sqs/<nome>" aceitando os headers recebidos
// como transporte de fila. Sem App configurado, é um no-op.
func (t TracerNewRelic) IniciarConsumo(ctx context.Context, nome string, headers map[string]string) (context.Context, func(error)) {
	if t.App == nil {
		return ctx, func(error) {}
	}
	txn := t.App.StartTransaction("sqs/" + nome)
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	txn.AcceptDistributedTraceHeaders(newrelic.TransportQueue, h)
	return newrelic.NewContext(ctx, txn), func(err error) {
		if err != nil {
			txn.NoticeError(err)
		}
		txn.End()
	}
}
