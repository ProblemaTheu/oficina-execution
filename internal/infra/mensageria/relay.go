package mensageria

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
)

// MensagemOutbox é uma linha pendente da outbox.
type MensagemOutbox struct {
	ID         uuid.UUID
	Destino    string // nome lógico do destino (ex.: "execution"), resolvido em URL pelo Relay
	Envelope   Envelope
	Trace      map[string]string
	Tentativas int
}

// OutboxStore é a porta de persistência da outbox usada pelo relay.
// A gravação (dentro da transação do efeito) fica no adaptador, fora desta
// porta, porque depende do tipo de transação de cada banco.
type OutboxStore interface {
	// TentarLideranca garante um único relay ativo por serviço, mesmo com várias
	// réplicas — é o que preserva a ordem por saga. Devolve lider=false sem erro
	// quando outra réplica já é a líder. liberar deve ser chamado ao fim do ciclo.
	TentarLideranca(ctx context.Context) (liberar func(), lider bool, err error)
	// Pendentes devolve até limite mensagens não enviadas e não desistidas, em
	// ordem de criação.
	Pendentes(ctx context.Context, limite int) ([]MensagemOutbox, error)
	// MarcarEnviada registra o envio com sucesso.
	MarcarEnviada(ctx context.Context, id uuid.UUID) error
	// RegistrarFalha incrementa as tentativas; com desistir=true a mensagem sai
	// da fila do relay (falhou_em) e deixa de travar as outras sagas.
	RegistrarFalha(ctx context.Context, id uuid.UUID, desistir bool) error
}

// publicador é a parte do Publisher usada pelo relay.
type publicador interface {
	Publicar(ctx context.Context, filaURL string, env Envelope, trace map[string]string) error
}

// ConfigRelay configura o Relay.
type ConfigRelay struct {
	// Destinos mapeia o destino lógico gravado na outbox para a URL da fila.
	Destinos map[string]string
	// Intervalo entre ciclos (padrão 500 ms).
	Intervalo time.Duration
	// Lote é o máximo de mensagens por ciclo (padrão 50).
	Lote int
	// MaxTentativas antes de desistir de uma mensagem (padrão 10).
	MaxTentativas int
}

// Relay publica as mensagens gravadas na outbox (backlog F4-4.2).
type Relay struct {
	store OutboxStore
	pub   publicador
	cfg   ConfigRelay
}

// NovoRelay cria o relay aplicando os padrões da configuração.
func NovoRelay(store OutboxStore, pub publicador, cfg ConfigRelay) *Relay {
	if cfg.Intervalo == 0 {
		cfg.Intervalo = 500 * time.Millisecond
	}
	if cfg.Lote == 0 {
		cfg.Lote = 50
	}
	if cfg.MaxTentativas == 0 {
		cfg.MaxTentativas = 10
	}
	return &Relay{store: store, pub: pub, cfg: cfg}
}

// Executar roda ciclos até ctx ser cancelado.
func (r *Relay) Executar(ctx context.Context) error {
	log := logging.Log(ctx)
	log.Info("relay da outbox iniciado", "intervalo", r.cfg.Intervalo.String())
	t := time.NewTicker(r.cfg.Intervalo)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("relay da outbox encerrado")
			return nil
		case <-t.C:
			if _, err := r.Ciclo(ctx); err != nil && ctx.Err() == nil {
				log.Warn("ciclo do relay interrompido", "error", err)
			}
		}
	}
}

// Ciclo publica um lote de pendentes e devolve quantas foram enviadas.
//
// Em erro transitório o ciclo PARA em vez de pular a mensagem: pular quebraria
// a ordem da saga. Depois de MaxTentativas, ou em erro permanente, a mensagem é
// desistida — sai da fila do relay com log de erro — para que uma linha
// venenosa não trave as outras sagas (head-of-line blocking).
func (r *Relay) Ciclo(ctx context.Context) (int, error) {
	liberar, lider, err := r.store.TentarLideranca(ctx)
	if err != nil {
		return 0, fmt.Errorf("liderança do relay: %w", err)
	}
	if !lider {
		return 0, nil
	}
	defer liberar()

	pendentes, err := r.store.Pendentes(ctx, r.cfg.Lote)
	if err != nil {
		return 0, fmt.Errorf("listar pendentes: %w", err)
	}

	enviadas := 0
	for _, m := range pendentes {
		log := logging.Log(ctx).With("outbox_id", m.ID, "destino", m.Destino, "saga_id", m.Envelope.SagaID,
			"message_id", m.Envelope.MessageID, "tipo_mensagem", m.Envelope.Tipo)

		fila, ok := r.cfg.Destinos[m.Destino]
		if !ok {
			log.Error("outbox: destino sem fila configurada; mensagem desistida")
			if err := r.store.RegistrarFalha(ctx, m.ID, true); err != nil {
				return enviadas, err
			}
			continue
		}

		if err := r.pub.Publicar(ctx, fila, m.Envelope, m.Trace); err != nil {
			desistir := EhPermanente(err) || m.Tentativas+1 >= r.cfg.MaxTentativas
			if errReg := r.store.RegistrarFalha(ctx, m.ID, desistir); errReg != nil {
				return enviadas, errReg
			}
			if desistir {
				log.Error("outbox: mensagem desistida", "error", err, "tentativas", m.Tentativas+1)
				continue
			}
			return enviadas, err
		}

		if err := r.store.MarcarEnviada(ctx, m.ID); err != nil {
			// Publicada mas não marcada: será republicada no próximo ciclo; o
			// MessageDeduplicationId e a inbox do destino absorvem a duplicata.
			return enviadas, fmt.Errorf("marcar enviada %s: %w", m.ID, err)
		}
		enviadas++
	}
	return enviadas, nil
}
