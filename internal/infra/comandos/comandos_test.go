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
	cmd     execucao.Comando
	p       execucao.PayloadIniciarDiagnostico
	reparo  execucao.PayloadExecutarReparo
	chamada string
	err     error
}

func (u *ucFake) ExecutarReparo(_ context.Context, cmd execucao.Comando, p execucao.PayloadExecutarReparo) error {
	u.cmd, u.reparo, u.chamada = cmd, p, "reparo"
	return u.err
}

func (u *ucFake) CancelarTarefas(_ context.Context, cmd execucao.Comando) error {
	u.cmd, u.chamada = cmd, "cancelar"
	return u.err
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

func TestExecutarReparoECancelarTarefas(t *testing.T) {
	uc := &ucFake{}
	hs := Handlers(uc)
	env := envelope(t, map[string]any{"numero_os": "OS-1", "veiculo": map[string]string{"placa": "FAL1A23"}, "diagnostico": "x"})
	env.Tipo = execucao.TipoExecutarReparo
	if err := hs[execucao.TipoExecutarReparo](context.Background(), env); err != nil || uc.reparo.Veiculo.Placa != "FAL1A23" || uc.chamada != "reparo" {
		t.Fatalf("ExecutarReparo: %v %+v", err, uc.reparo)
	}
	env.Payload = json.RawMessage(`{"veiculo":7}`)
	_ = hs[execucao.TipoExecutarReparo](context.Background(), env)
	if uc.reparo.Veiculo.Placa != "" {
		t.Error("payload inválido segue vazio (o caso de uso responde ExecucaoFalhou)")
	}
	cancelar := envelope(t, map[string]string{"motivo": "compensacao_T6"})
	if err := hs[execucao.TipoCancelarTarefas](context.Background(), cancelar); err != nil || uc.chamada != "cancelar" || uc.cmd.OsID != cancelar.OsID {
		t.Errorf("CancelarTarefas: %v %s", err, uc.chamada)
	}
	dup := Handlers(&ucFake{err: execucao.ErrJaProcessada})
	if err := dup[execucao.TipoCancelarTarefas](context.Background(), cancelar); err != nil {
		t.Errorf("duplicata vira ack: %v", err)
	}
}
