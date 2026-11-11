package mensageria

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// AtributoTipo é o message attribute com o tipo da mensagem — permite rotear
// no consumidor sem desserializar o corpo.
const AtributoTipo = "tipo"

// maxAtributosTrace limita os headers de trace copiados como message
// attributes: o SQS aceita 10 por mensagem e um já é o tipo.
const maxAtributosTrace = 9

// ClienteSQS é o subconjunto do cliente SQS usado pelo pacote — permite
// testar sem AWS. *sqs.Client satisfaz a interface.
type ClienteSQS interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Publisher publica envelopes em filas SQS FIFO.
type Publisher struct {
	sqs ClienteSQS
}

// NovoPublisher cria o publisher sobre o cliente SQS.
func NovoPublisher(c ClienteSQS) *Publisher {
	return &Publisher{sqs: c}
}

// Publicar envia o envelope para a fila FIFO.
//
//   - MessageGroupId = saga_id: ordem garantida dentro de uma saga, sagas
//     diferentes em paralelo.
//   - MessageDeduplicationId = message_id: o relay pode republicar a mesma
//     linha da outbox (crash entre o envio e o UPDATE) sem duplicar dentro da
//     janela de 5 min do FIFO; depois dela, quem protege é a inbox.
func (p *Publisher) Publicar(ctx context.Context, filaURL string, env Envelope, trace map[string]string) error {
	if err := env.Validar(); err != nil {
		return Permanente(err)
	}
	corpo, err := json.Marshal(env)
	if err != nil {
		return Permanente(fmt.Errorf("serializar envelope %s: %w", env.MessageID, err))
	}
	_, err = p.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(filaURL),
		MessageBody:            aws.String(string(corpo)),
		MessageGroupId:         aws.String(env.SagaID.String()),
		MessageDeduplicationId: aws.String(env.MessageID.String()),
		MessageAttributes:      atributos(env.Tipo, trace),
	})
	if err != nil {
		return fmt.Errorf("publicar %s (%s) em %s: %w", env.Tipo, env.MessageID, filaURL, err)
	}
	return nil
}

// atributos monta os message attributes: o tipo e, até o limite do SQS, os
// headers de trace com valor não vazio.
func atributos(tipo string, trace map[string]string) map[string]types.MessageAttributeValue {
	attrs := map[string]types.MessageAttributeValue{
		AtributoTipo: {DataType: aws.String("String"), StringValue: aws.String(tipo)},
	}
	n := 0
	for k, v := range trace {
		if v == "" || k == AtributoTipo || n == maxAtributosTrace {
			continue
		}
		attrs[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
		n++
	}
	return attrs
}
