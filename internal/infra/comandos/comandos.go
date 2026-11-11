// Package comandos liga os comandos da fila execution-commands.fifo aos casos
// de uso — é o mapa tipo → handler entregue ao mensageria.Consumer.
package comandos

import (
	"context"
	"errors"

	"github.com/ProblemaTheu/oficina-execution/internal/application/execucao"
	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
	"github.com/ProblemaTheu/oficina-execution/internal/infra/mensageria"
)

// casosDeUso é o que os handlers usam do execucao.Servico.
type casosDeUso interface {
	IniciarDiagnostico(ctx context.Context, cmd execucao.Comando, p execucao.PayloadIniciarDiagnostico) error
	ExecutarReparo(ctx context.Context, cmd execucao.Comando, p execucao.PayloadExecutarReparo) error
	CancelarTarefas(ctx context.Context, cmd execucao.Comando) error
}

// Handlers devolve os handlers por tipo de mensagem.
func Handlers(uc casosDeUso) map[string]mensageria.Handler {
	return map[string]mensageria.Handler{
		execucao.TipoIniciarDiagnostico: func(ctx context.Context, env mensageria.Envelope) error {
			var p execucao.PayloadIniciarDiagnostico
			if err := env.LerPayload(&p); err != nil {
				// Payload que não desserializa segue com payload vazio: o caso de
				// uso responde DiagnosticoFalhou e a saga compensa. Descartar em
				// silêncio deixaria a saga parada esperando resposta.
				logging.Log(ctx).Error("payload de IniciarDiagnostico inválido", "error", err)
				p = execucao.PayloadIniciarDiagnostico{}
			}
			return ignorarDuplicata(uc.IniciarDiagnostico(ctx, comando(env), p))
		},
		execucao.TipoExecutarReparo: func(ctx context.Context, env mensageria.Envelope) error {
			var p execucao.PayloadExecutarReparo
			if err := env.LerPayload(&p); err != nil {
				// Vazio → o caso de uso responde ExecucaoFalhou (a saga estorna).
				logging.Log(ctx).Error("payload de ExecutarReparo inválido", "error", err)
				p = execucao.PayloadExecutarReparo{}
			}
			return ignorarDuplicata(uc.ExecutarReparo(ctx, comando(env), p))
		},
		execucao.TipoCancelarTarefas: func(ctx context.Context, env mensageria.Envelope) error {
			return ignorarDuplicata(uc.CancelarTarefas(ctx, comando(env)))
		},
	}
}

func comando(env mensageria.Envelope) execucao.Comando {
	return execucao.Comando{MessageID: env.MessageID, Tipo: env.Tipo, SagaID: env.SagaID, OsID: env.OsID}
}

// ignorarDuplicata: comando já processado é sucesso — o consumidor dá ack.
func ignorarDuplicata(err error) error {
	if errors.Is(err, execucao.ErrJaProcessada) {
		return nil
	}
	return err
}
