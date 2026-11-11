// Package tarefa modela a fila de execução da oficina: tarefas de diagnóstico
// e de reparo de uma OS e seus estados (backlog F4-3.2).
package tarefa

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Tipo da tarefa.
type Tipo string

const (
	Diagnostico Tipo = "DIAGNOSTICO"
	Reparo      Tipo = "REPARO"
)

// Valido informa se o tipo existe.
func (t Tipo) Valido() bool { return t == Diagnostico || t == Reparo }

// Prioridade na fila: reparo antes de diagnóstico — carro já pago esperando é
// o pior cenário para a oficina. Menor = antes.
func (t Tipo) Prioridade() int {
	if t == Reparo {
		return 1
	}
	return 2
}

// Estado da tarefa.
type Estado string

const (
	Aguardando  Estado = "AGUARDANDO"
	EmAndamento Estado = "EM_ANDAMENTO"
	Concluida   Estado = "CONCLUIDA"
	Cancelada   Estado = "CANCELADA"
)

// Erros de regra de negócio (viram 4xx na API).
var (
	ErrTransicaoInvalida   = errors.New("transição de estado inválida")
	ErrTarefaCancelada     = errors.New("tarefa cancelada")
	ErrDiagnosticoVazio    = errors.New("diagnóstico é obrigatório")
	ErrTipoErrado          = errors.New("operação não se aplica a este tipo de tarefa")
	ErrMecanicoObrigatorio = errors.New("mecânico é obrigatório para iniciar a tarefa")
)

// Tarefa é uma unidade de trabalho da oficina vinculada a uma OS e à saga dela.
type Tarefa struct {
	ID           uuid.UUID
	OsID         uuid.UUID
	SagaID       uuid.UUID
	NumeroOS     string
	Tipo         Tipo
	Estado       Estado
	Placa        string
	Modelo       string
	Descricao    string
	Mecanico     string // sub do JWT de quem iniciou
	Diagnostico  string
	CriadoEm     time.Time
	IniciadoEm   *time.Time
	FinalizadoEm *time.Time
}

// Nova cria uma tarefa aguardando na fila.
func Nova(tipo Tipo, osID, sagaID uuid.UUID, numeroOS, placa, modelo, descricao string, agora time.Time) (Tarefa, error) {
	if !tipo.Valido() {
		return Tarefa{}, fmt.Errorf("tipo de tarefa %q inválido", tipo)
	}
	if osID == uuid.Nil || sagaID == uuid.Nil {
		return Tarefa{}, errors.New("os_id e saga_id são obrigatórios")
	}
	return Tarefa{
		ID: uuid.New(), OsID: osID, SagaID: sagaID, NumeroOS: numeroOS, Tipo: tipo, Estado: Aguardando,
		Placa: strings.ToUpper(strings.TrimSpace(placa)), Modelo: modelo, Descricao: descricao, CriadoEm: agora.UTC(),
	}, nil
}

// Iniciar coloca a tarefa em andamento com o mecânico responsável.
func (t *Tarefa) Iniciar(mecanico string, agora time.Time) error {
	if t.Estado == Cancelada {
		return ErrTarefaCancelada
	}
	if t.Estado != Aguardando {
		return fmt.Errorf("%w: %s → %s", ErrTransicaoInvalida, t.Estado, EmAndamento)
	}
	if strings.TrimSpace(mecanico) == "" {
		return ErrMecanicoObrigatorio
	}
	inicio := agora.UTC()
	t.Estado, t.Mecanico, t.IniciadoEm = EmAndamento, mecanico, &inicio
	return nil
}

// ConcluirDiagnostico encerra uma tarefa de diagnóstico registrando o laudo.
// Aceita a partir de AGUARDANDO também: o mecânico pode concluir direto, e o
// iniciado_em fica igual ao finalizado_em.
func (t *Tarefa) ConcluirDiagnostico(diagnostico, mecanico string, agora time.Time) error {
	if t.Tipo != Diagnostico {
		return ErrTipoErrado
	}
	if strings.TrimSpace(diagnostico) == "" {
		return ErrDiagnosticoVazio
	}
	if t.Estado == Aguardando {
		if err := t.Iniciar(mecanico, agora); err != nil {
			return err
		}
	}
	if t.Estado == Cancelada {
		return ErrTarefaCancelada
	}
	if t.Estado != EmAndamento {
		return fmt.Errorf("%w: %s → %s", ErrTransicaoInvalida, t.Estado, Concluida)
	}
	fim := agora.UTC()
	t.Estado, t.Diagnostico, t.FinalizadoEm = Concluida, strings.TrimSpace(diagnostico), &fim
	return nil
}
