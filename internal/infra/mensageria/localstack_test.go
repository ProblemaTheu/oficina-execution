//go:build localstack

// Testes do pacote contra SQS FIFO de verdade (LocalStack). Exigem
// AWS_ENDPOINT_URL apontando para o LocalStack:
//
//	docker compose -f docker-compose.saga.yml up -d aws
//	AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
//	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//	go test -tags localstack ./internal/infra/mensageria/
//
// Provam o que o mock não prova: deduplicação real do FIFO, ordem por grupo,
// atributos de trace atravessando a fila e redrive para a DLQ.
package mensageria_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/infra/mensageria"
)

func clienteSQS(t *testing.T) *sqs.Client {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL") == "" {
		t.Skip("AWS_ENDPOINT_URL não definido — suba o LocalStack (docker-compose.saga.yml)")
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Fatalf("config AWS: %v", err)
	}
	return sqs.NewFromConfig(cfg)
}

// filaDeTeste cria uma fila FIFO exclusiva do teste, com DLQ e visibility curto
// (para o teste de redrive não esperar 60 s por tentativa).
func filaDeTeste(t *testing.T, c *sqs.Client, maxReceive int) (fila, dlq string) {
	t.Helper()
	ctx := context.Background()
	nome := fmt.Sprintf("teste-%s", uuid.NewString()[:8])

	d, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(nome + "-dlq.fifo"),
		Attributes: map[string]string{"FifoQueue": "true"},
	})
	if err != nil {
		t.Fatalf("criar DLQ: %v", err)
	}
	attrs, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: d.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(nome + ".fifo"),
		Attributes: map[string]string{
			"FifoQueue":         "true",
			"VisibilityTimeout": "1",
			"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"%d"}`,
				attrs.Attributes["QueueArn"], maxReceive),
		},
	})
	if err != nil {
		t.Fatalf("criar fila: %v", err)
	}
	t.Cleanup(func() {
		_, _ = c.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: f.QueueUrl})
		_, _ = c.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: d.QueueUrl})
	})
	return aws.ToString(f.QueueUrl), aws.ToString(d.QueueUrl)
}

func receber(t *testing.T, c *sqs.Client, fila string) []types.Message {
	t.Helper()
	out, err := c.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(fila), MaxNumberOfMessages: 10, WaitTimeSeconds: 2,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		t.Fatalf("receber: %v", err)
	}
	return out.Messages
}

func novoEnvelope(t *testing.T, saga uuid.UUID, tipo string) mensageria.Envelope {
	t.Helper()
	env, err := mensageria.Novo(tipo, saga, uuid.New(), "req-ls", mensageria.OrigemOS, map[string]string{"x": "y"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestLocalStack_PublicarEConsumirComTrace(t *testing.T) {
	c := clienteSQS(t)
	fila, _ := filaDeTeste(t, c, 5)
	env := novoEnvelope(t, uuid.New(), "IniciarDiagnostico")

	if err := mensageria.NovoPublisher(c).Publicar(context.Background(), fila, env,
		map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}); err != nil {
		t.Fatalf("Publicar: %v", err)
	}

	msgs := receber(t, c, fila)
	if len(msgs) != 1 {
		t.Fatalf("recebidas %d, esperava 1", len(msgs))
	}
	if aws.ToString(msgs[0].MessageAttributes["traceparent"].StringValue) == "" {
		t.Error("header de trace não atravessou a fila")
	}

	var recebido mensageria.Envelope
	cons := mensageria.NovoConsumer(c, mensageria.ConfigConsumer{FilaURL: fila, Handlers: map[string]mensageria.Handler{
		"IniciarDiagnostico": func(_ context.Context, e mensageria.Envelope) error { recebido = e; return nil },
	}})
	if r := cons.Processar(context.Background(), msgs[0]); r != mensageria.Confirmada {
		t.Fatalf("resultado = %s", r)
	}
	if recebido.MessageID != env.MessageID {
		t.Error("handler não recebeu o envelope publicado")
	}
	if sobrou := receber(t, c, fila); len(sobrou) != 0 {
		t.Errorf("mensagem confirmada continuou na fila: %d", len(sobrou))
	}
}

func TestLocalStack_DeduplicacaoPorMessageID(t *testing.T) {
	c := clienteSQS(t)
	fila, _ := filaDeTeste(t, c, 5)
	env := novoEnvelope(t, uuid.New(), "GerarOrcamento")
	pub := mensageria.NovoPublisher(c)

	// O relay republica a mesma linha se cair entre o envio e o UPDATE.
	for i := 0; i < 3; i++ {
		if err := pub.Publicar(context.Background(), fila, env, nil); err != nil {
			t.Fatal(err)
		}
	}
	if msgs := receber(t, c, fila); len(msgs) != 1 {
		t.Errorf("FIFO deveria deduplicar por message_id: recebidas %d", len(msgs))
	}
}

func TestLocalStack_OrdemDentroDaSaga(t *testing.T) {
	c := clienteSQS(t)
	fila, _ := filaDeTeste(t, c, 5)
	saga := uuid.New()
	pub := mensageria.NovoPublisher(c)

	var enviados []uuid.UUID
	for i := 0; i < 5; i++ {
		env := novoEnvelope(t, saga, fmt.Sprintf("Etapa%d", i))
		enviados = append(enviados, env.MessageID)
		if err := pub.Publicar(context.Background(), fila, env, nil); err != nil {
			t.Fatal(err)
		}
	}

	var recebidos []uuid.UUID
	for len(recebidos) < 5 {
		msgs := receber(t, c, fila)
		if len(msgs) == 0 {
			t.Fatalf("faltaram mensagens: %d de 5", len(recebidos))
		}
		for _, m := range msgs {
			env, err := mensageria.Decodificar([]byte(aws.ToString(m.Body)))
			if err != nil {
				t.Fatal(err)
			}
			recebidos = append(recebidos, env.MessageID)
			_, _ = c.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(fila), ReceiptHandle: m.ReceiptHandle})
		}
	}
	for i := range enviados {
		if recebidos[i] != enviados[i] {
			t.Fatalf("posição %d fora de ordem", i)
		}
	}
}

// Critério de aceite do F4-4.1 / DoD: mensagem venenosa vai para a DLQ.
func TestLocalStack_ErroTransitorioRepetidoVaiParaDLQ(t *testing.T) {
	c := clienteSQS(t)
	fila, dlq := filaDeTeste(t, c, 2)
	env := novoEnvelope(t, uuid.New(), "EstornarPagamento")
	if err := mensageria.NovoPublisher(c).Publicar(context.Background(), fila, env, nil); err != nil {
		t.Fatal(err)
	}

	cons := mensageria.NovoConsumer(c, mensageria.ConfigConsumer{FilaURL: fila, Handlers: map[string]mensageria.Handler{
		"EstornarPagamento": func(context.Context, mensageria.Envelope) error { return fmt.Errorf("mercado pago 503") },
	}})

	prazo := time.Now().Add(20 * time.Second)
	for time.Now().Before(prazo) {
		for _, m := range receber(t, c, fila) {
			if r := cons.Processar(context.Background(), m); r != mensageria.Devolvida {
				t.Fatalf("erro transitório deve devolver a mensagem, veio %s", r)
			}
		}
		if msgs := receber(t, c, dlq); len(msgs) == 1 {
			lido, err := mensageria.Decodificar([]byte(aws.ToString(msgs[0].Body)))
			if err != nil || lido.MessageID != env.MessageID {
				t.Fatalf("mensagem errada na DLQ: %v", err)
			}
			return
		}
	}
	t.Fatal("mensagem não chegou à DLQ após maxReceiveCount")
}
