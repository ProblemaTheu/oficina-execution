package mensageria

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
)

const filaExecution = "http://aws:4566/000000000000/execution-commands.fifo"

func TestPublicar_FifoPorSagaEDeduplicacaoPorMessageID(t *testing.T) {
	f := &sqsFake{}
	env := envelopeValido(t, "IniciarDiagnostico")

	err := NovoPublisher(f).Publicar(context.Background(), filaExecution, env, map[string]string{
		"traceparent": "00-abc-def-01", "tracestate": "", "newrelic": "eyJ2Ijp",
	})
	if err != nil {
		t.Fatalf("Publicar: %v", err)
	}
	if len(f.enviadas) != 1 {
		t.Fatalf("enviadas = %d", len(f.enviadas))
	}
	in := f.enviadas[0]
	if aws.ToString(in.QueueUrl) != filaExecution {
		t.Errorf("fila = %s", aws.ToString(in.QueueUrl))
	}
	if aws.ToString(in.MessageGroupId) != env.SagaID.String() {
		t.Errorf("MessageGroupId deve ser o saga_id, veio %s", aws.ToString(in.MessageGroupId))
	}
	if aws.ToString(in.MessageDeduplicationId) != env.MessageID.String() {
		t.Errorf("MessageDeduplicationId deve ser o message_id, veio %s", aws.ToString(in.MessageDeduplicationId))
	}
	if aws.ToString(in.MessageAttributes[AtributoTipo].StringValue) != "IniciarDiagnostico" {
		t.Error("atributo tipo ausente")
	}
	if _, ok := in.MessageAttributes["tracestate"]; ok {
		t.Error("header de trace vazio não deve virar atributo")
	}
	if aws.ToString(in.MessageAttributes["traceparent"].StringValue) != "00-abc-def-01" {
		t.Error("traceparent não propagado")
	}

	var corpo Envelope
	if err := json.Unmarshal([]byte(aws.ToString(in.MessageBody)), &corpo); err != nil || corpo.MessageID != env.MessageID {
		t.Errorf("corpo não é o envelope: %v", err)
	}
}

func TestPublicar_EnvelopeInvalidoEhPermanente(t *testing.T) {
	f := &sqsFake{}
	env := envelopeValido(t, "X")
	env.SagaID = uuid.Nil

	err := NovoPublisher(f).Publicar(context.Background(), filaExecution, env, nil)
	if !EhPermanente(err) {
		t.Fatalf("esperava erro permanente, veio %v", err)
	}
	if len(f.enviadas) != 0 {
		t.Error("envelope inválido não pode ser enviado")
	}
}

func TestPublicar_ErroDoSQSEhTransitorio(t *testing.T) {
	f := &sqsFake{erroEnvio: errInfra}
	err := NovoPublisher(f).Publicar(context.Background(), filaExecution, envelopeValido(t, "X"), nil)
	if err == nil || EhPermanente(err) {
		t.Fatalf("erro do SQS deve ser transitório, veio %v", err)
	}
}

func TestAtributos_RespeitaLimiteDoSQS(t *testing.T) {
	trace := map[string]string{AtributoTipo: "sobrescrever"}
	for i := 0; i < 20; i++ {
		trace[fmt.Sprintf("h%02d", i)] = "v"
	}
	attrs := atributos("Tipo", trace)
	if len(attrs) != 10 {
		t.Errorf("SQS aceita 10 atributos, veio %d", len(attrs))
	}
	if aws.ToString(attrs[AtributoTipo].StringValue) != "Tipo" {
		t.Error("header de trace não pode sobrescrever o tipo")
	}
}
