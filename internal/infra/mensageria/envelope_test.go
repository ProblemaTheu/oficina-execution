package mensageria

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNovo_PreencheContrato(t *testing.T) {
	saga, os := uuid.New(), uuid.New()
	agora := time.Date(2026, 11, 10, 11, 3, 22, 0, time.FixedZone("BRT", -3*3600))

	env, err := Novo("IniciarDiagnostico", saga, os, "req-9", OrigemOS, map[string]string{"placa": "ABC1D23"}, agora)
	if err != nil {
		t.Fatalf("Novo: %v", err)
	}
	if env.MessageID == uuid.Nil || env.Versao != VersaoAtual || env.SagaID != saga || env.OsID != os {
		t.Errorf("identificadores/versão errados: %+v", env)
	}
	if env.OcorridoEm.Location() != time.UTC || !env.OcorridoEm.Equal(agora) {
		t.Errorf("ocorrido_em deve ser o mesmo instante em UTC, veio %v", env.OcorridoEm)
	}
	var p map[string]string
	if err := env.LerPayload(&p); err != nil || p["placa"] != "ABC1D23" {
		t.Errorf("payload = %v (%v)", p, err)
	}
}

func TestNovo_PayloadNilViraObjetoVazio(t *testing.T) {
	env, err := Novo("TarefasCanceladas", uuid.New(), uuid.New(), "", OrigemExecution, nil, time.Now())
	if err != nil {
		t.Fatalf("Novo: %v", err)
	}
	if string(env.Payload) != "{}" {
		t.Errorf("payload = %s, esperava {}", env.Payload)
	}
}

func TestNovo_PayloadNaoSerializavel(t *testing.T) {
	_, err := Novo("X", uuid.New(), uuid.New(), "", OrigemOS, map[string]any{"f": func() {}}, time.Now())
	if err == nil {
		t.Fatal("esperava erro ao serializar uma função")
	}
}

func TestNovo_ValidaOrigem(t *testing.T) {
	_, err := Novo("X", uuid.New(), uuid.New(), "", "lambda", struct{}{}, time.Now())
	if !errors.Is(err, ErrEnvelopeInvalido) {
		t.Fatalf("esperava ErrEnvelopeInvalido, veio %v", err)
	}
}

func TestValidar_CamposObrigatorios(t *testing.T) {
	base := Envelope{
		MessageID: uuid.New(), Tipo: "T", Versao: 1, SagaID: uuid.New(), OsID: uuid.New(),
		OcorridoEm: time.Now(), Origem: OrigemBilling, Payload: json.RawMessage(`{}`),
	}
	if err := base.Validar(); err != nil {
		t.Fatalf("envelope base deveria ser válido: %v", err)
	}
	casos := map[string]func(*Envelope){
		"message_id": func(e *Envelope) { e.MessageID = uuid.Nil },
		"tipo":       func(e *Envelope) { e.Tipo = "" },
		"versao":     func(e *Envelope) { e.Versao = 0 },
		"saga_id":    func(e *Envelope) { e.SagaID = uuid.Nil },
		"os_id":      func(e *Envelope) { e.OsID = uuid.Nil },
		"ocorrido":   func(e *Envelope) { e.OcorridoEm = time.Time{} },
		"origem":     func(e *Envelope) { e.Origem = "" },
		"payload":    func(e *Envelope) { e.Payload = nil },
	}
	for nome, estragar := range casos {
		t.Run(nome, func(t *testing.T) {
			e := base
			estragar(&e)
			if err := e.Validar(); !errors.Is(err, ErrEnvelopeInvalido) {
				t.Errorf("esperava ErrEnvelopeInvalido, veio %v", err)
			}
		})
	}
}

func TestDecodificar_RoundTrip(t *testing.T) {
	env := envelopeValido(t, "GerarOrcamento")
	corpo, _ := json.Marshal(env)

	lido, err := Decodificar(corpo)
	if err != nil {
		t.Fatalf("Decodificar: %v", err)
	}
	if lido.MessageID != env.MessageID || lido.Tipo != env.Tipo || string(lido.Payload) != string(env.Payload) {
		t.Errorf("round-trip divergiu: %+v × %+v", lido, env)
	}
}

func TestDecodificar_Invalidos(t *testing.T) {
	for nome, corpo := range map[string]string{
		"json quebrado":   `{"tipo":`,
		"sem saga":        fmt.Sprintf(`{"message_id":%q,"tipo":"T","versao":1,"os_id":%q,"ocorrido_em":"2026-11-10T00:00:00Z","origem":"os-service","payload":{}}`, uuid.New(), uuid.New()),
		"uuid malformado": `{"message_id":"xyz"}`,
	} {
		t.Run(nome, func(t *testing.T) {
			if _, err := Decodificar([]byte(corpo)); !errors.Is(err, ErrEnvelopeInvalido) {
				t.Errorf("esperava ErrEnvelopeInvalido, veio %v", err)
			}
		})
	}
}

func TestLerPayload_TipoErradoEhPermanente(t *testing.T) {
	env := envelopeValido(t, "T")
	var destino struct {
		NumeroOS int `json:"numero_os"` // no payload é string
	}
	err := env.LerPayload(&destino)
	if err == nil || !EhPermanente(err) {
		t.Fatalf("esperava erro permanente, veio %v", err)
	}
}

func TestPermanente(t *testing.T) {
	if Permanente(nil) != nil {
		t.Error("Permanente(nil) deve ser nil")
	}
	causa := errors.New("payload inválido")
	err := fmt.Errorf("handler: %w", Permanente(causa))
	if !EhPermanente(err) || !errors.Is(err, causa) || err.Error() != "handler: payload inválido" {
		t.Errorf("embrulho perdeu a causa ou a classificação: %v", err)
	}
	if EhPermanente(errInfra) {
		t.Error("erro comum não é permanente")
	}
}
