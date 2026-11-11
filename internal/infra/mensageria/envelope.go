// Package mensageria implementa o transporte das mensagens da saga da OS sobre
// Amazon SQS FIFO: envelope comum, publicação, consumo e o relay da outbox.
//
// O pacote não conhece banco de dados. A outbox é acessada pela porta
// OutboxStore (implementada em internal/infra/repository para PostgreSQL) e a
// idempotência do consumo (inbox) fica no handler, dentro da mesma transação do
// efeito. Assim o mesmo pacote é copiado sem alteração para o Billing
// (PostgreSQL) e o Execution (DynamoDB) — ver ADR-013.
//
// Contrato das mensagens: docs/planejamentos/fase-4/arquitetura.md §5.
package mensageria

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// VersaoAtual é a versão do envelope gravada nas mensagens novas.
const VersaoAtual = 1

// Origens válidas no campo Envelope.Origem.
const (
	OrigemOS        = "os-service"
	OrigemBilling   = "billing-service"
	OrigemExecution = "execution-service"
)

// ErrEnvelopeInvalido indica um envelope malformado. Reentregar não resolve:
// o consumidor descarta a mensagem com log de erro (ver Consumer).
var ErrEnvelopeInvalido = errors.New("envelope inválido")

// Envelope é o formato comum de todas as mensagens da saga.
type Envelope struct {
	MessageID     uuid.UUID       `json:"message_id"`
	Tipo          string          `json:"tipo"`
	Versao        int             `json:"versao"`
	SagaID        uuid.UUID       `json:"saga_id"`
	OsID          uuid.UUID       `json:"os_id"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	OcorridoEm    time.Time       `json:"ocorrido_em"`
	Origem        string          `json:"origem"`
	Payload       json.RawMessage `json:"payload"`
}

// Novo monta um envelope com message_id novo. O payload é serializado aqui
// para que um payload não serializável falhe na origem, e não no consumidor.
func Novo(tipo string, sagaID, osID uuid.UUID, correlationID, origem string, payload any, agora time.Time) (Envelope, error) {
	if payload == nil {
		payload = struct{}{}
	}
	corpo, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("serializar payload de %s: %w", tipo, err)
	}
	env := Envelope{
		MessageID:     uuid.New(),
		Tipo:          tipo,
		Versao:        VersaoAtual,
		SagaID:        sagaID,
		OsID:          osID,
		CorrelationID: correlationID,
		OcorridoEm:    agora.UTC(),
		Origem:        origem,
		Payload:       corpo,
	}
	if err := env.Validar(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Validar confere os campos obrigatórios do contrato.
func (e Envelope) Validar() error {
	switch {
	case e.MessageID == uuid.Nil:
		return fmt.Errorf("%w: message_id vazio", ErrEnvelopeInvalido)
	case e.Tipo == "":
		return fmt.Errorf("%w: tipo vazio", ErrEnvelopeInvalido)
	case e.Versao < 1:
		return fmt.Errorf("%w: versao %d", ErrEnvelopeInvalido, e.Versao)
	case e.SagaID == uuid.Nil:
		return fmt.Errorf("%w: saga_id vazio", ErrEnvelopeInvalido)
	case e.OsID == uuid.Nil:
		return fmt.Errorf("%w: os_id vazio", ErrEnvelopeInvalido)
	case e.OcorridoEm.IsZero():
		return fmt.Errorf("%w: ocorrido_em vazio", ErrEnvelopeInvalido)
	case e.Origem != OrigemOS && e.Origem != OrigemBilling && e.Origem != OrigemExecution:
		return fmt.Errorf("%w: origem %q", ErrEnvelopeInvalido, e.Origem)
	case len(e.Payload) == 0:
		return fmt.Errorf("%w: payload vazio", ErrEnvelopeInvalido)
	}
	return nil
}

// Decodificar lê um envelope do corpo de uma mensagem e o valida.
func Decodificar(corpo []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(corpo, &env); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrEnvelopeInvalido, err)
	}
	if err := env.Validar(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// LerPayload desserializa o payload no tipo do destino. Payload que não casa
// com o tipo esperado é erro permanente — reentregar não o conserta.
func (e Envelope) LerPayload(destino any) error {
	if err := json.Unmarshal(e.Payload, destino); err != nil {
		return Permanente(fmt.Errorf("payload de %s: %w", e.Tipo, err))
	}
	return nil
}
