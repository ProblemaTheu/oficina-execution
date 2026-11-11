package comandos

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/application/execucao"
	"github.com/ProblemaTheu/oficina-execution/internal/infra/mensageria"
)

type ucFake struct {
	cmd execucao.Comando
	p   execucao.PayloadIniciarDiagnostico
	err error
}

func (u *ucFake) IniciarDiagnostico(_ context.Context, cmd execucao.Comando, p execucao.PayloadIniciarDiagnostico) error {
	u.cmd, u.p = cmd, p
	return u.err
}

func envelope(t *testing.T, payload any) mensageria.Envelope {
	t.Helper()
	env, err := mensageria.Novo(execucao.TipoIniciarDiagnostico, uuid.New(), uuid.New(), "req", mensageria.OrigemOS, payload, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestIniciarDiagnostico_RepassaComandoEPayload(t *testing.T) {
	uc := &ucFake{}
	env := envelope(t, map[string]any{"numero_os": "OS-1", "veiculo": map[string]string{"placa": "ABC1D23", "modelo": "Gol"}})

	if err := Handlers(uc)[execucao.TipoIniciarDiagnostico](context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if uc.cmd.MessageID != env.MessageID || uc.cmd.SagaID != env.SagaID || uc.cmd.OsID != env.OsID {
		t.Errorf("comando = %+v", uc.cmd)
	}
	if uc.p.Veiculo.Placa != "ABC1D23" || uc.p.NumeroOS != "OS-1" {
		t.Errorf("payload = %+v", uc.p)
	}
}

func TestIniciarDiagnostico_PayloadInvalidoSegueVazioParaResponderFalha(t *testing.T) {
	uc := &ucFake{}
	env := envelope(t, nil)
	env.Payload = json.RawMessage(`{"veiculo":"nao-e-objeto"}`)

	if err := Handlers(uc)[execucao.TipoIniciarDiagnostico](context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if uc.p.Veiculo.Placa != "" || uc.cmd.MessageID != env.MessageID {
		t.Error("payload inválido deve chegar vazio ao caso de uso (que responde DiagnosticoFalhou)")
	}
}

func TestIniciarDiagnostico_ClassificaErros(t *testing.T) {
	h := func(err error) error {
		return Handlers(&ucFake{err: err})[execucao.TipoIniciarDiagnostico](context.Background(), envelope(t, map[string]string{}))
	}
	if err := h(execucao.ErrJaProcessada); err != nil {
		t.Errorf("duplicata deve virar ack: %v", err)
	}
	infra := errors.New("dynamodb fora")
	if err := h(infra); !errors.Is(err, infra) {
		t.Errorf("erro de infra deve voltar (reentrega): %v", err)
	}
}
