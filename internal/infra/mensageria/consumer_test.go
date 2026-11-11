package mensageria

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
)

func mensagemSQS(t *testing.T, env Envelope, attrs map[string]string) types.Message {
	t.Helper()
	corpo, _ := json.Marshal(env)
	ma := map[string]types.MessageAttributeValue{
		AtributoTipo: {DataType: aws.String("String"), StringValue: aws.String(env.Tipo)},
	}
	for k, v := range attrs {
		ma[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	return types.Message{
		MessageId:         aws.String("m-" + env.MessageID.String()),
		ReceiptHandle:     aws.String("rh-" + env.MessageID.String()),
		Body:              aws.String(string(corpo)),
		MessageAttributes: ma,
		Attributes:        map[string]string{"ApproximateReceiveCount": "1"},
	}
}

// tracerEspiao registra o que o consumidor passou ao tracer.
type tracerEspiao struct {
	nome     string
	headers  map[string]string
	errFinal error
	fechado  bool
}

func (*tracerEspiao) Capturar(context.Context) map[string]string { return nil }
func (s *tracerEspiao) IniciarConsumo(ctx context.Context, nome string, h map[string]string) (context.Context, func(error)) {
	s.nome, s.headers = nome, h
	return ctx, func(err error) { s.errFinal, s.fechado = err, true }
}

func TestProcessar_Classificacao(t *testing.T) {
	errNegocioPermanente := Permanente(errors.New("payload sem placa"))
	casos := []struct {
		nome       string
		errHandler error
		esperado   Resultado
		removida   bool
	}{
		{"sucesso confirma", nil, Confirmada, true},
		{"erro permanente descarta", errNegocioPermanente, Descartada, true},
		{"erro de infra devolve", errInfra, Devolvida, false},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			f := &sqsFake{}
			env := envelopeValido(t, "IniciarDiagnostico")
			var recebido Envelope
			tr := &tracerEspiao{}
			cons := NovoConsumer(f, ConfigConsumer{FilaURL: filaExecution, Tracer: tr, Handlers: map[string]Handler{
				"IniciarDiagnostico": func(ctx context.Context, e Envelope) error {
					recebido = e
					if logging.Correlacao(ctx) != "req-1" {
						t.Errorf("correlation_id do envelope deve ir para o contexto, veio %q", logging.Correlacao(ctx))
					}
					if logging.Log(ctx) == nil {
						t.Error("handler deve receber o logger correlacionado")
					}
					return c.errHandler
				},
			}})

			r := cons.Processar(context.Background(), mensagemSQS(t, env, map[string]string{"traceparent": "00-a-b-01"}))

			if r != c.esperado {
				t.Errorf("resultado = %s, esperava %s", r, c.esperado)
			}
			if got := f.totalRemovidas() == 1; got != c.removida {
				t.Errorf("removida = %v, esperava %v", got, c.removida)
			}
			if recebido.MessageID != env.MessageID {
				t.Error("handler não recebeu o envelope")
			}
			if tr.nome != "IniciarDiagnostico" || tr.headers["traceparent"] != "00-a-b-01" || !tr.fechado {
				t.Errorf("trace não propagado: %+v", tr)
			}
			if _, ok := tr.headers[AtributoTipo]; ok {
				t.Error("atributo tipo não é header de trace")
			}
			if !errors.Is(tr.errFinal, c.errHandler) {
				t.Errorf("erro do handler não chegou ao tracer: %v", tr.errFinal)
			}
		})
	}
}

func TestProcessar_EnvelopeInvalidoEhDescartado(t *testing.T) {
	f := &sqsFake{}
	cons := NovoConsumer(f, ConfigConsumer{FilaURL: filaExecution})
	msg := types.Message{ReceiptHandle: aws.String("rh"), Body: aws.String(`{"tipo":"X"}`)}

	if r := cons.Processar(context.Background(), msg); r != Descartada {
		t.Fatalf("resultado = %s", r)
	}
	if f.totalRemovidas() != 1 {
		t.Error("mensagem malformada deve sair da fila (reentregar não conserta)")
	}
}

func TestProcessar_TipoSemHandlerEhDescartado(t *testing.T) {
	f := &sqsFake{}
	cons := NovoConsumer(f, ConfigConsumer{FilaURL: filaExecution, Handlers: map[string]Handler{}})
	if r := cons.Processar(context.Background(), mensagemSQS(t, envelopeValido(t, "Desconhecido"), nil)); r != Descartada {
		t.Fatalf("resultado = %s", r)
	}
}

func TestProcessar_FalhaNoDeleteMantemResultado(t *testing.T) {
	f := &sqsFake{erroRemove: errInfra}
	cons := NovoConsumer(f, ConfigConsumer{FilaURL: filaExecution, Handlers: map[string]Handler{
		"T": func(context.Context, Envelope) error { return nil },
	}})
	// A mensagem volta após o visibility timeout; a inbox absorve a duplicata.
	if r := cons.Processar(context.Background(), mensagemSQS(t, envelopeValido(t, "T"), nil)); r != Confirmada {
		t.Fatalf("resultado = %s", r)
	}
}

func TestExecutar_ProcessaEmOrdemEEncerraNoCancelamento(t *testing.T) {
	e1, e2, e3 := envelopeValido(t, "T"), envelopeValido(t, "T"), envelopeValido(t, "T")
	f := &sqsFake{recebimentos: []recebimento{
		{err: errInfra}, // falha transitória no ReceiveMessage: espera e tenta de novo
		{msgs: []types.Message{mensagemSQS(t, e1, nil), mensagemSQS(t, e2, nil)}},
		{msgs: []types.Message{mensagemSQS(t, e3, nil)}},
	}}
	ctx, cancelar := context.WithCancel(context.Background())
	var ordem []string
	cons := NovoConsumer(f, ConfigConsumer{FilaURL: filaExecution, PausaAposErro: time.Millisecond, Handlers: map[string]Handler{
		"T": func(ctx context.Context, e Envelope) error {
			if ctx.Err() != nil {
				t.Error("handler não deve herdar o cancelamento do consumidor")
			}
			ordem = append(ordem, e.MessageID.String())
			if len(ordem) == 3 {
				cancelar()
			}
			return nil
		},
	}})

	feito := make(chan error, 1)
	go func() { feito <- cons.Executar(ctx) }()
	select {
	case err := <-feito:
		if err != nil {
			t.Fatalf("Executar: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumidor não encerrou após o cancelamento")
	}

	want := []string{e1.MessageID.String(), e2.MessageID.String(), e3.MessageID.String()}
	if len(ordem) != 3 || ordem[0] != want[0] || ordem[1] != want[1] || ordem[2] != want[2] {
		t.Errorf("ordem = %v, esperava %v", ordem, want)
	}
}

func TestExecutar_CancelamentoNoMeioDoLoteParaDeProcessar(t *testing.T) {
	e1, e2 := envelopeValido(t, "T"), envelopeValido(t, "T")
	f := &sqsFake{recebimentos: []recebimento{{msgs: []types.Message{mensagemSQS(t, e1, nil), mensagemSQS(t, e2, nil)}}}}
	ctx, cancelar := context.WithCancel(context.Background())
	processadas := 0
	cons := NovoConsumer(f, ConfigConsumer{FilaURL: filaExecution, Handlers: map[string]Handler{
		"T": func(context.Context, Envelope) error { processadas++; cancelar(); return nil },
	}})

	if err := cons.Executar(ctx); err != nil {
		t.Fatal(err)
	}
	// A 1ª termina (contexto sem cancelamento); a 2ª volta à fila após o visibility timeout.
	if processadas != 1 || f.totalRemovidas() != 1 {
		t.Errorf("processadas = %d, removidas = %d; esperava 1 e 1", processadas, f.totalRemovidas())
	}
}

func TestResultado_String(t *testing.T) {
	if Confirmada.String() != "confirmada" || Descartada.String() != "descartada" || Devolvida.String() != "devolvida" {
		t.Error("nomes dos resultados mudaram — são usados nos logs")
	}
}

func TestEsperar_RespeitaCancelamento(t *testing.T) {
	ctx, cancelar := context.WithCancel(context.Background())
	cancelar()
	inicio := time.Now()
	esperar(ctx, time.Hour)
	if time.Since(inicio) > time.Second {
		t.Error("esperar deve retornar quando o contexto é cancelado")
	}
}
