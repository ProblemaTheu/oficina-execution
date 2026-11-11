// Package execucao contém os casos de uso do Execution Service: reagir aos
// comandos da saga (fila de execução) e às ações do mecânico, respondendo ao
// orquestrador do OS Service pela outbox.
package execucao

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
	"github.com/ProblemaTheu/oficina-execution/internal/domain/tarefa"
)

// Tipos de mensagem do catálogo (arquitetura §5.2 do oficina-app).
const (
	TipoIniciarDiagnostico   = "IniciarDiagnostico"
	TipoDiagnosticoIniciado  = "DiagnosticoIniciado"
	TipoDiagnosticoConcluido = "DiagnosticoConcluido"
	TipoDiagnosticoFalhou    = "DiagnosticoFalhou"
	TipoExecutarReparo       = "ExecutarReparo"
	TipoExecucaoIniciada     = "ExecucaoIniciada"
	TipoExecucaoFalhou       = "ExecucaoFalhou"
	TipoExecucaoConcluida    = "ExecucaoConcluida"
	TipoCancelarTarefas      = "CancelarTarefas"
	TipoTarefasCanceladas    = "TarefasCanceladas"
)

// Erros do repositório traduzidos pela API e pelo consumidor.
var (
	ErrNaoEncontrada = errors.New("tarefa não encontrada")
	ErrConflito      = errors.New("a tarefa foi alterada por outra operação")
	ErrJaProcessada  = errors.New("mensagem já processada")
	ErrTipoInvalido  = errors.New("tipo de tarefa inválido")
)

// Saida é uma resposta ao orquestrador, gravada na outbox junto com o efeito.
type Saida struct {
	Tipo    string
	SagaID  uuid.UUID
	OsID    uuid.UUID
	Payload any
}

// Comando identifica uma mensagem recebida da saga (para a inbox).
type Comando struct {
	MessageID uuid.UUID
	Tipo      string
	SagaID    uuid.UUID
	OsID      uuid.UUID
}

// Repositorio é a porta de persistência. Cada escrita é atômica com a inbox
// (quando há comando) e com as saídas da outbox.
type Repositorio interface {
	// CriarDeComando grava a tarefa, marca o comando na inbox e grava as saídas.
	// ErrJaProcessada se o comando já foi processado.
	CriarDeComando(ctx context.Context, t tarefa.Tarefa, cmd Comando, saidas []Saida) error
	// Responder marca o comando na inbox e grava as saídas, sem tarefa
	// (ex.: comando inválido respondido com falha). ErrJaProcessada idem.
	Responder(ctx context.Context, cmd Comando, saidas []Saida) error
	// Salvar regrava a tarefa se o estado ainda for estadoAnterior, com as
	// saídas. ErrConflito se outra operação mudou a tarefa no meio.
	Salvar(ctx context.Context, t tarefa.Tarefa, estadoAnterior tarefa.Estado, saidas []Saida) error
	// SalvarVariasDeComando regrava várias tarefas (cada uma condicionada ao
	// seu estado anterior), marca o comando na inbox e grava as saídas — tudo
	// ou nada. ErrJaProcessada se o comando já foi processado; ErrConflito se
	// alguma tarefa mudou no meio.
	SalvarVariasDeComando(ctx context.Context, ts []tarefa.Tarefa, anteriores []tarefa.Estado, cmd Comando, saidas []Saida) error
	Buscar(ctx context.Context, id uuid.UUID) (tarefa.Tarefa, error)
	ListarFila(ctx context.Context, tipo tarefa.Tipo) ([]tarefa.Tarefa, error)
	ListarPorOS(ctx context.Context, osID uuid.UUID) ([]tarefa.Tarefa, error)
}

// Servico reúne os casos de uso.
type Servico struct {
	repo          Repositorio
	agora         func() time.Time
	falhaSimulada bool
}

// Opcao configura o serviço.
type Opcao func(*Servico)

// ComFalhaSimulada liga a falha de demonstração por placa FAL* (F4-4.7).
// Desligada por padrão; só a ligue no ambiente de gravação do vídeo.
func ComFalhaSimulada(ligada bool) Opcao { return func(s *Servico) { s.falhaSimulada = ligada } }

// Novo cria o serviço.
func Novo(repo Repositorio, opts ...Opcao) *Servico {
	s := &Servico{repo: repo, agora: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

// PayloadIniciarDiagnostico é o comando enviado pelo orquestrador.
type PayloadIniciarDiagnostico struct {
	NumeroOS string `json:"numero_os"`
	Veiculo  struct {
		Placa  string `json:"placa"`
		Modelo string `json:"modelo"`
	} `json:"veiculo"`
	Descricao string `json:"descricao"`
}

// IniciarDiagnostico cria a tarefa de diagnóstico na fila.
//
// Comando sem placa é erro de negócio: responde DiagnosticoFalhou (a saga
// compensa) e o consumidor dá ack — reentregar não conserta o payload.
// Comando repetido devolve ErrJaProcessada, que o consumidor trata como ack.
func (s *Servico) IniciarDiagnostico(ctx context.Context, cmd Comando, p PayloadIniciarDiagnostico) error {
	if strings.TrimSpace(p.Veiculo.Placa) == "" {
		logging.Log(ctx).Warn("IniciarDiagnostico sem placa; respondendo falha", "os_id", cmd.OsID)
		return s.repo.Responder(ctx, cmd, []Saida{{
			Tipo: TipoDiagnosticoFalhou, SagaID: cmd.SagaID, OsID: cmd.OsID,
			Payload: map[string]string{"motivo": "comando_invalido: veiculo.placa ausente"},
		}})
	}
	t, err := tarefa.Nova(tarefa.Diagnostico, cmd.OsID, cmd.SagaID, p.NumeroOS, p.Veiculo.Placa, p.Veiculo.Modelo, p.Descricao, s.agora())
	if err != nil {
		return fmt.Errorf("nova tarefa: %w", err)
	}
	if err := s.repo.CriarDeComando(ctx, t, cmd, nil); err != nil {
		return err
	}
	logging.Log(ctx).Info("tarefa de diagnóstico na fila", "tarefa_id", t.ID, "os_id", t.OsID)
	return nil
}

// Iniciar coloca a tarefa em andamento com o mecânico do token.
func (s *Servico) Iniciar(ctx context.Context, id uuid.UUID, mecanico string) (tarefa.Tarefa, error) {
	t, err := s.repo.Buscar(ctx, id)
	if err != nil {
		return tarefa.Tarefa{}, err
	}
	anterior := t.Estado
	if err := t.Iniciar(mecanico, s.agora()); err != nil {
		return tarefa.Tarefa{}, err
	}
	var saidas []Saida
	switch t.Tipo {
	case tarefa.Diagnostico:
		// Informativo: o orquestrador muda a OS para em_diagnostico.
		saidas = append(saidas, Saida{Tipo: TipoDiagnosticoIniciado, SagaID: t.SagaID, OsID: t.OsID,
			Payload: map[string]string{"tarefa_id": t.ID.String(), "mecanico": mecanico}})
	case tarefa.Reparo:
		// Pivô da saga: o OS baixa a reserva de estoque; daqui não se compensa.
		saidas = append(saidas, Saida{Tipo: TipoExecucaoIniciada, SagaID: t.SagaID, OsID: t.OsID,
			Payload: map[string]string{"tarefa_id": t.ID.String(), "iniciado_em": t.IniciadoEm.Format(time.RFC3339)}})
	}
	if err := s.repo.Salvar(ctx, t, anterior, saidas); err != nil {
		return tarefa.Tarefa{}, err
	}
	return t, nil
}

// ConcluirDiagnostico registra o laudo e responde DiagnosticoConcluido.
func (s *Servico) ConcluirDiagnostico(ctx context.Context, id uuid.UUID, mecanico, diagnostico string) (tarefa.Tarefa, error) {
	t, err := s.repo.Buscar(ctx, id)
	if err != nil {
		return tarefa.Tarefa{}, err
	}
	anterior := t.Estado
	if err := t.ConcluirDiagnostico(diagnostico, mecanico, s.agora()); err != nil {
		return tarefa.Tarefa{}, err
	}
	saidas := []Saida{{Tipo: TipoDiagnosticoConcluido, SagaID: t.SagaID, OsID: t.OsID,
		Payload: map[string]string{"tarefa_id": t.ID.String(), "diagnostico": t.Diagnostico}}}
	if err := s.repo.Salvar(ctx, t, anterior, saidas); err != nil {
		return tarefa.Tarefa{}, err
	}
	logging.Log(ctx).Info("diagnóstico concluído", "tarefa_id", t.ID, "os_id", t.OsID)
	return t, nil
}

// ListarFila devolve as tarefas aguardando de um tipo (ou de todos), na ordem
// de atendimento: reparo antes de diagnóstico; dentro do tipo, por chegada.
func (s *Servico) ListarFila(ctx context.Context, tipo tarefa.Tipo) ([]tarefa.Tarefa, error) {
	tipos := []tarefa.Tipo{tarefa.Reparo, tarefa.Diagnostico}
	if tipo != "" {
		if !tipo.Valido() {
			return nil, fmt.Errorf("%w: %q", ErrTipoInvalido, tipo)
		}
		tipos = []tarefa.Tipo{tipo}
	}
	var fila []tarefa.Tarefa
	for _, tp := range tipos {
		ts, err := s.repo.ListarFila(ctx, tp)
		if err != nil {
			return nil, err
		}
		fila = append(fila, ts...)
	}
	sort.SliceStable(fila, func(i, j int) bool {
		if fila[i].Tipo.Prioridade() != fila[j].Tipo.Prioridade() {
			return fila[i].Tipo.Prioridade() < fila[j].Tipo.Prioridade()
		}
		return fila[i].CriadoEm.Before(fila[j].CriadoEm)
	})
	return fila, nil
}

// ListarPorOS devolve as tarefas de uma OS.
func (s *Servico) ListarPorOS(ctx context.Context, osID uuid.UUID) ([]tarefa.Tarefa, error) {
	return s.repo.ListarPorOS(ctx, osID)
}

// ── Reparo e cancelamento ─────────────────────────────────────────────────────

// PayloadExecutarReparo é o comando enviado após o pagamento confirmado.
type PayloadExecutarReparo struct {
	NumeroOS string `json:"numero_os"`
	Veiculo  struct {
		Placa  string `json:"placa"`
		Modelo string `json:"modelo"`
	} `json:"veiculo"`
	Diagnostico string `json:"diagnostico"`
}

// ExecutarReparo põe o reparo na fila (com prioridade sobre diagnóstico).
//
// Falha simulada (F4-4.7): com a opção ligada e placa FAL*, responde
// ExecucaoFalhou — a saga, que já tem o pagamento, estorna. É a cena do vídeo.
func (s *Servico) ExecutarReparo(ctx context.Context, cmd Comando, p PayloadExecutarReparo) error {
	falha := func(motivo string) error {
		return s.repo.Responder(ctx, cmd, []Saida{{Tipo: TipoExecucaoFalhou, SagaID: cmd.SagaID, OsID: cmd.OsID,
			Payload: map[string]string{"motivo": motivo}}})
	}
	if strings.TrimSpace(p.Veiculo.Placa) == "" {
		logging.Log(ctx).Warn("ExecutarReparo sem placa; respondendo falha", "os_id", cmd.OsID)
		return falha("comando_invalido: veiculo.placa ausente")
	}
	if s.falhaSimulada && tarefa.PlacaDeFalhaSimulada(p.Veiculo.Placa) {
		logging.Log(ctx).Warn("falha simulada no reparo (placa FAL*)", "os_id", cmd.OsID, "placa", p.Veiculo.Placa)
		return falha("falha_simulada")
	}
	t, err := tarefa.Nova(tarefa.Reparo, cmd.OsID, cmd.SagaID, p.NumeroOS, p.Veiculo.Placa, p.Veiculo.Modelo, p.Diagnostico, s.agora())
	if err != nil {
		return fmt.Errorf("nova tarefa: %w", err)
	}
	if err := s.repo.CriarDeComando(ctx, t, cmd, nil); err != nil {
		return err
	}
	logging.Log(ctx).Info("tarefa de reparo na fila", "tarefa_id", t.ID, "os_id", t.OsID)
	return nil
}

// ConcluirReparo encerra o reparo e responde ExecucaoConcluida (OS finalizada).
func (s *Servico) ConcluirReparo(ctx context.Context, id uuid.UUID) (tarefa.Tarefa, error) {
	t, err := s.repo.Buscar(ctx, id)
	if err != nil {
		return tarefa.Tarefa{}, err
	}
	anterior := t.Estado
	if err := t.ConcluirReparo(s.agora()); err != nil {
		return tarefa.Tarefa{}, err
	}
	saidas := []Saida{{Tipo: TipoExecucaoConcluida, SagaID: t.SagaID, OsID: t.OsID,
		Payload: map[string]string{"tarefa_id": t.ID.String(), "finalizado_em": t.FinalizadoEm.Format(time.RFC3339)}}}
	if err := s.repo.Salvar(ctx, t, anterior, saidas); err != nil {
		return tarefa.Tarefa{}, err
	}
	logging.Log(ctx).Info("reparo concluído", "tarefa_id", t.ID, "os_id", t.OsID)
	return t, nil
}

// CancelarTarefas (compensação): cancela as tarefas não concluídas da OS e
// responde TarefasCanceladas{quantidade} — zero é resposta válida, e é com ela
// que o orquestrador sabe que pode seguir a compensação.
func (s *Servico) CancelarTarefas(ctx context.Context, cmd Comando) error {
	ts, err := s.repo.ListarPorOS(ctx, cmd.OsID)
	if err != nil {
		return err
	}
	var (
		canceladas []tarefa.Tarefa
		anteriores []tarefa.Estado
	)
	for _, t := range ts {
		anterior := t.Estado
		if t.Cancelar(s.agora()) {
			canceladas, anteriores = append(canceladas, t), append(anteriores, anterior)
		}
	}
	resposta := []Saida{{Tipo: TipoTarefasCanceladas, SagaID: cmd.SagaID, OsID: cmd.OsID,
		Payload: map[string]int{"quantidade": len(canceladas)}}}
	if err := s.repo.SalvarVariasDeComando(ctx, canceladas, anteriores, cmd, resposta); err != nil {
		return err
	}
	logging.Log(ctx).Info("tarefas canceladas pela compensação", "os_id", cmd.OsID, "quantidade", len(canceladas))
	return nil
}
