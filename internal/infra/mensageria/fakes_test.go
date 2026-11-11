package mensageria

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

var errInfra = errors.New("sqs fora do ar")

// sqsFake grava as chamadas e devolve respostas programadas.
type sqsFake struct {
	mu sync.Mutex

	enviadas   []*sqs.SendMessageInput
	erroEnvio  error
	removidas  []string
	erroRemove error

	// recebimentos é consumido em ordem; depois dele, ReceiveMessage bloqueia até o ctx acabar.
	recebimentos []recebimento
	recebeuN     int
}

type recebimento struct {
	msgs []types.Message
	err  error
}

func (f *sqsFake) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.erroEnvio != nil {
		return nil, f.erroEnvio
	}
	f.enviadas = append(f.enviadas, in)
	return &sqs.SendMessageOutput{MessageId: aws.String("sqs-" + aws.ToString(in.MessageDeduplicationId))}, nil
}

func (f *sqsFake) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	if f.recebeuN < len(f.recebimentos) {
		r := f.recebimentos[f.recebeuN]
		f.recebeuN++
		f.mu.Unlock()
		return &sqs.ReceiveMessageOutput{Messages: r.msgs}, r.err
	}
	f.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *sqsFake) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.erroRemove != nil {
		return nil, f.erroRemove
	}
	f.removidas = append(f.removidas, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *sqsFake) totalRemovidas() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.removidas)
}

// envelopeValido monta um envelope válido para os testes.
func envelopeValido(t *testing.T, tipo string) Envelope {
	t.Helper()
	env, err := Novo(tipo, uuid.New(), uuid.New(), "req-1", OrigemOS,
		map[string]any{"numero_os": "OS-2026-00001"}, time.Date(2026, 11, 10, 14, 3, 22, 0, time.UTC))
	if err != nil {
		t.Fatalf("Novo: %v", err)
	}
	return env
}
