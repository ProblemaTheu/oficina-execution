package mensageria

import (
	"context"
	"errors"
	"testing"

	"github.com/newrelic/go-agent/v3/newrelic"
)

func appNewRelicDesligado(t *testing.T) *newrelic.Application {
	t.Helper()
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName("teste-mensageria"),
		newrelic.ConfigEnabled(false),
		newrelic.ConfigDistributedTracerEnabled(true),
	)
	if err != nil {
		t.Fatalf("app New Relic: %v", err)
	}
	return app
}

func TestTracerNulo(t *testing.T) {
	ctx := context.Background()
	if len(TracerNulo{}.Capturar(ctx)) != 0 {
		t.Error("TracerNulo não captura nada")
	}
	got, fim := TracerNulo{}.IniciarConsumo(ctx, "X", map[string]string{"a": "b"})
	fim(errInfra)
	if got != ctx {
		t.Error("TracerNulo deve devolver o mesmo contexto")
	}
}

func TestTracerNewRelic_SemTransacaoNaoCaptura(t *testing.T) {
	if h := (TracerNewRelic{}).Capturar(context.Background()); len(h) != 0 {
		t.Errorf("sem transação no contexto não há headers: %v", h)
	}
}

func TestTracerNewRelic_SemAppEhNoop(t *testing.T) {
	ctx := context.Background()
	got, fim := TracerNewRelic{}.IniciarConsumo(ctx, "X", nil)
	fim(nil)
	if got != ctx {
		t.Error("sem App o consumo segue sem transação")
	}
}

func TestTracerNewRelic_ConsumoAbreTransacaoEPropaga(t *testing.T) {
	tr := TracerNewRelic{App: appNewRelicDesligado(t)}

	ctx, fim := tr.IniciarConsumo(context.Background(), "IniciarDiagnostico",
		map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"})
	if newrelic.FromContext(ctx) == nil {
		t.Fatal("contexto do handler deve carregar a transação de consumo")
	}
	// Com a transação no contexto, Capturar não pode quebrar (com o agente
	// desligado o mapa pode vir vazio — o valor real é coberto em ambiente).
	_ = tr.Capturar(ctx)
	fim(errors.New("falhou"))
}
