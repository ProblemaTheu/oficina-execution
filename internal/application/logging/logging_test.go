package logging

import (
	"context"
	"log/slog"
	"testing"
)

func TestLog_SemLoggerNoContextoDevolveODefault(t *testing.T) {
	if got := Log(context.Background()); got != slog.Default() {
		t.Fatal("fora de uma requisição, Log deve devolver slog.Default()")
	}
}

func TestComLogger_RoundTrip(t *testing.T) {
	logger := slog.Default().With("correlation_id", "abc")
	ctx := ComLogger(context.Background(), logger)
	if got := Log(ctx); got != logger {
		t.Fatal("Log não devolveu o logger colocado por ComLogger")
	}
}

func TestCorrelacao_RoundTrip(t *testing.T) {
	if Correlacao(context.Background()) != "" {
		t.Error("fora de uma requisição o correlation_id é vazio")
	}
	if got := Correlacao(ComCorrelacao(context.Background(), "req-1")); got != "req-1" {
		t.Errorf("Correlacao = %q", got)
	}
}
