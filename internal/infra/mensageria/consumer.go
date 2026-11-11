package mensageria

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
)

// Handler processa uma mensagem de um tipo. Deve ser idempotente: o SQS entrega
// pelo menos uma vez. A forma recomendada é registrar o message_id na inbox na
// mesma transação do efeito e devolver nil quando ele já existir.
type Handler func(ctx context.Context, env Envelope) error

// Resultado do processamento de uma mensagem.
type Resultado int

const (
	// Confirmada: processada com sucesso (ou duplicata) e removida da fila.
	Confirmada Resultado = iota
	// Descartada: erro permanente; removida da fila com log de erro.
	Descartada
	// Devolvida: erro transitório; fica na fila para reentrega (e DLQ após N).
	Devolvida
)

func (r Resultado) String() string {
	switch r {
	case Confirmada:
		return "confirmada"
	case Descartada:
		return "descartada"
	default:
		return "devolvida"
	}
}

// ConfigConsumer configura um Consumer.
type ConfigConsumer struct {
	FilaURL  string
	Handlers map[string]Handler
	Tracer   Tracer // nil = TracerNulo
	// EsperaLongPolling é o WaitTimeSeconds (padrão 20 — long polling evita
	// ~260 mil requisições/dia por consumidor ocioso; ver roadmap).
	EsperaLongPolling int32
	// Lote é o MaxNumberOfMessages (padrão 10).
	Lote int32
	// PausaAposErro é a espera após uma falha no ReceiveMessage (padrão 2 s).
	PausaAposErro time.Duration
}

// Consumer consome uma fila SQS e despacha cada mensagem para o handler do seu tipo.
type Consumer struct {
	sqs ClienteSQS
	cfg ConfigConsumer
}

// NovoConsumer cria o consumidor aplicando os padrões da configuração.
func NovoConsumer(c ClienteSQS, cfg ConfigConsumer) *Consumer {
	if cfg.Tracer == nil {
		cfg.Tracer = TracerNulo{}
	}
	if cfg.EsperaLongPolling == 0 {
		cfg.EsperaLongPolling = 20
	}
	if cfg.Lote == 0 {
		cfg.Lote = 10
	}
	if cfg.PausaAposErro == 0 {
		cfg.PausaAposErro = 2 * time.Second
	}
	return &Consumer{sqs: c, cfg: cfg}
}

// Executar consome até ctx ser cancelado.
//
// Shutdown gracioso (F4-4.3): ao cancelar, para de receber e termina a mensagem
// em processamento — com um contexto que não herda o cancelamento — antes de
// sair. As demais mensagens do lote voltam a ficar visíveis após o visibility
// timeout. Por isso o terminationGracePeriodSeconds do pod deve ser maior que
// o tempo máximo de processamento de uma mensagem.
func (c *Consumer) Executar(ctx context.Context) error {
	log := logging.Log(ctx).With("fila", c.cfg.FilaURL)
	log.Info("consumidor iniciado")
	for {
		if ctx.Err() != nil {
			log.Info("consumidor encerrado")
			return nil
		}
		out, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(c.cfg.FilaURL),
			MaxNumberOfMessages:         c.cfg.Lote,
			WaitTimeSeconds:             c.cfg.EsperaLongPolling,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				continue
			}
			log.Warn("falha ao receber mensagens", "error", err)
			esperar(ctx, c.cfg.PausaAposErro)
			continue
		}
		for _, msg := range out.Messages {
			// Mensagens do mesmo grupo chegam em ordem no lote: processar em
			// sequência preserva a ordem da saga.
			c.Processar(context.WithoutCancel(ctx), msg)
			if ctx.Err() != nil {
				break
			}
		}
	}
}

// Processar trata uma mensagem recebida e decide entre confirmar, descartar ou
// devolver. Exportado para testes e para reprocessamento manual.
func (c *Consumer) Processar(ctx context.Context, msg types.Message) Resultado {
	log := logging.Log(ctx).With("fila", c.cfg.FilaURL, "sqs_message_id", aws.ToString(msg.MessageId),
		"tentativa", msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])

	env, err := Decodificar([]byte(aws.ToString(msg.Body)))
	if err != nil {
		log.Error("mensagem descartada: envelope inválido", "error", err)
		return c.confirmar(ctx, log, msg, Descartada)
	}

	log = log.With("saga_id", env.SagaID, "os_id", env.OsID, "message_id", env.MessageID,
		"tipo_mensagem", env.Tipo, "origem", env.Origem)
	if env.CorrelationID != "" {
		log = log.With("correlation_id", env.CorrelationID)
	}

	handler, ok := c.cfg.Handlers[env.Tipo]
	if !ok {
		log.Error("mensagem descartada: tipo sem handler")
		return c.confirmar(ctx, log, msg, Descartada)
	}

	ctx = logging.ComLogger(ctx, log)
	if env.CorrelationID != "" {
		// A resposta que o handler gravar na outbox sai com o mesmo correlation_id.
		ctx = logging.ComCorrelacao(ctx, env.CorrelationID)
	}
	ctx, fim := c.cfg.Tracer.IniciarConsumo(ctx, env.Tipo, headersDeTrace(msg))
	err = handler(ctx, env)
	fim(err)

	switch {
	case err == nil:
		log.Info("mensagem processada")
		return c.confirmar(ctx, log, msg, Confirmada)
	case EhPermanente(err):
		log.Error("mensagem descartada: erro permanente no handler", "error", err)
		return c.confirmar(ctx, log, msg, Descartada)
	default:
		log.Warn("mensagem devolvida para reentrega", "error", err)
		return Devolvida
	}
}

// confirmar remove a mensagem da fila. Se o DeleteMessage falhar, a mensagem
// volta após o visibility timeout e a inbox do handler absorve a duplicata.
func (c *Consumer) confirmar(ctx context.Context, log *slog.Logger, msg types.Message, r Resultado) Resultado {
	_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.cfg.FilaURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		log.Warn("falha ao remover mensagem da fila; será reentregue", "error", err, "resultado", r.String())
	}
	return r
}

// headersDeTrace extrai os message attributes que não são o tipo.
func headersDeTrace(msg types.Message) map[string]string {
	h := make(map[string]string, len(msg.MessageAttributes))
	for k, v := range msg.MessageAttributes {
		if k == AtributoTipo || v.StringValue == nil {
			continue
		}
		h[k] = *v.StringValue
	}
	return h
}

// esperar dorme d ou até ctx ser cancelado.
func esperar(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
