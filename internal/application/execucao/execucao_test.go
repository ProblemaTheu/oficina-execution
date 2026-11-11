package execucao

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/domain/tarefa"
)

// repoMemoria imita o contrato do adaptador DynamoDB: inbox por message_id,
// escrita condicionada ao estado anterior e saídas gravadas junto do efeito.
type repoMemoria struct {
	tarefas    map[uuid.UUID]tarefa.Tarefa
	inbox      map[uuid.UUID]bool
	saidas     []Saida
	errEscrita error
	errLeitura error
}

func novoRepo() *repoMemoria {
	return &repoMemoria{tarefas: map[uuid.UUID]tarefa.Tarefa{}, inbox: map[uuid.UUID]bool{}}
}

func (r *repoMemoria) marcar(cmd Comando) error {
	if r.inbox[cmd.MessageID] {
		return ErrJaProcessada
	}
	r.inbox[cmd.MessageID] = true
	return nil
}

func (r *repoMemoria) CriarDeComando(_ context.Context, t tarefa.Tarefa, cmd Comando, s []Saida) error {
	if r.errEscrita != nil {
		return r.errEscrita
	}
	if err := r.marcar(cmd); err != nil {
		return err
	}
	r.tarefas[t.ID] = t
	r.saidas = append(r.saidas, s...)
	return nil
}

func (r *repoMemoria) Responder(_ context.Context, cmd Comando, s []Saida) error {
	if r.errEscrita != nil {
		return r.errEscrita
	}
	if err := r.marcar(cmd); err != nil {
		return err
	}
	r.saidas = append(r.saidas, s...)
	return nil
}

func (r *repoMemoria) Salvar(_ context.Context, t tarefa.Tarefa, anterior tarefa.Estado, s []Saida) error {
	if r.errEscrita != nil {
		return r.errEscrita
	}
	if r.tarefas[t.ID].Estado != anterior {
		return ErrConflito
	}
	r.tarefas[t.ID] = t
	r.saidas = append(r.saidas, s...)
	return nil
}

func (r *repoMemoria) SalvarVariasDeComando(_ context.Context, ts []tarefa.Tarefa, anteriores []tarefa.Estado, cmd Comando, s []Saida) error {
	if r.errEscrita != nil {
		return r.errEscrita
	}
	if r.inbox[cmd.MessageID] {
		return ErrJaProcessada
	}
	for i, t := range ts {
		if r.tarefas[t.ID].Estado != anteriores[i] {
			return ErrConflito
		}
	}
	r.inbox[cmd.MessageID] = true
	for _, t := range ts {
		r.tarefas[t.ID] = t
	}
	r.saidas = append(r.saidas, s...)
	return nil
}

func (r *repoMemoria) Buscar(_ context.Context, id uuid.UUID) (tarefa.Tarefa, error) {
	if r.errLeitura != nil {
		return tarefa.Tarefa{}, r.errLeitura
	}
	t, ok := r.tarefas[id]
	if !ok {
		return tarefa.Tarefa{}, ErrNaoEncontrada
	}
	return t, nil
}

func (r *repoMemoria) ListarFila(_ context.Context, tipo tarefa.Tipo) ([]tarefa.Tarefa, error) {
	if r.errLeitura != nil {
		return nil, r.errLeitura
	}
	var out []tarefa.Tarefa
	for _, t := range r.tarefas {
		if t.Tipo == tipo && t.Estado == tarefa.Aguardando {
			out = append(out, t)
		}
	}
	return out, nil
}

func (r *repoMemoria) ListarPorOS(_ context.Context, osID uuid.UUID) ([]tarefa.Tarefa, error) {
	if r.errLeitura != nil {
		return nil, r.errLeitura
	}
	var out []tarefa.Tarefa
	for _, t := range r.tarefas {
		if t.OsID == osID {
			out = append(out, t)
		}
	}
	return out, nil
}

var relogio = time.Date(2026, 11, 11, 15, 0, 0, 0, time.UTC)

func servico(r *repoMemoria) *Servico {
	s := Novo(r)
	s.agora = func() time.Time { return relogio }
	return s
}

func comando() Comando {
	return Comando{MessageID: uuid.New(), Tipo: TipoIniciarDiagnostico, SagaID: uuid.New(), OsID: uuid.New()}
}

func payload(placa string) PayloadIniciarDiagnostico {
	var p PayloadIniciarDiagnostico
	p.NumeroOS, p.Veiculo.Placa, p.Veiculo.Modelo, p.Descricao = "OS-2026-00001", placa, "Gol", "barulho"
	return p
}

func unica(t *testing.T, r *repoMemoria) tarefa.Tarefa {
	t.Helper()
	if len(r.tarefas) != 1 {
		t.Fatalf("esperava 1 tarefa, há %d", len(r.tarefas))
	}
	for _, tf := range r.tarefas {
		return tf
	}
	return tarefa.Tarefa{}
}

func TestIniciarDiagnostico_CriaTarefaNaFila(t *testing.T) {
	r := novoRepo()
	cmd := comando()

	if err := servico(r).IniciarDiagnostico(context.Background(), cmd, payload("ABC1D23")); err != nil {
		t.Fatal(err)
	}
	tf := unica(t, r)
	if tf.Tipo != tarefa.Diagnostico || tf.Estado != tarefa.Aguardando || tf.OsID != cmd.OsID || tf.SagaID != cmd.SagaID || tf.Placa != "ABC1D23" {
		t.Errorf("tarefa errada: %+v", tf)
	}
	if len(r.saidas) != 0 {
		t.Error("criar a tarefa não responde nada à saga (quem responde é o mecânico)")
	}
}

func TestIniciarDiagnostico_ComandoRepetidoEhJaProcessada(t *testing.T) {
	r := novoRepo()
	cmd := comando()
	s := servico(r)
	_ = s.IniciarDiagnostico(context.Background(), cmd, payload("ABC1D23"))

	err := s.IniciarDiagnostico(context.Background(), cmd, payload("ABC1D23"))

	if !errors.Is(err, ErrJaProcessada) || len(r.tarefas) != 1 {
		t.Fatalf("reentrega duplicou a tarefa: err=%v tarefas=%d", err, len(r.tarefas))
	}
}

func TestIniciarDiagnostico_SemPlacaRespondeFalha(t *testing.T) {
	r := novoRepo()
	cmd := comando()

	if err := servico(r).IniciarDiagnostico(context.Background(), cmd, payload(" ")); err != nil {
		t.Fatalf("erro de negócio não é erro de infraestrutura: %v", err)
	}
	if len(r.tarefas) != 0 || len(r.saidas) != 1 || r.saidas[0].Tipo != TipoDiagnosticoFalhou || r.saidas[0].SagaID != cmd.SagaID {
		t.Fatalf("esperava só DiagnosticoFalhou: tarefas=%d saídas=%+v", len(r.tarefas), r.saidas)
	}
	if !r.inbox[cmd.MessageID] {
		t.Error("a falha também marca a inbox (reentrega não responde duas vezes)")
	}
}

func TestIniciarDiagnostico_ErroDeEscritaPropaga(t *testing.T) {
	r := novoRepo()
	r.errEscrita = errors.New("dynamodb fora")
	if err := servico(r).IniciarDiagnostico(context.Background(), comando(), payload("ABC1D23")); err == nil {
		t.Fatal("erro de infra deve voltar para o consumidor (sem ack → reentrega)")
	}
}

func TestIniciarEConcluirDiagnostico_RespondeASaga(t *testing.T) {
	r := novoRepo()
	s := servico(r)
	cmd := comando()
	_ = s.IniciarDiagnostico(context.Background(), cmd, payload("ABC1D23"))
	id := unica(t, r).ID

	if _, err := s.Iniciar(context.Background(), id, "mec-1"); err != nil {
		t.Fatal(err)
	}
	tf, err := s.ConcluirDiagnostico(context.Background(), id, "mec-1", " troca do filtro ")
	if err != nil {
		t.Fatal(err)
	}

	if tf.Estado != tarefa.Concluida || tf.Diagnostico != "troca do filtro" {
		t.Errorf("tarefa = %+v", tf)
	}
	if len(r.saidas) != 2 || r.saidas[0].Tipo != TipoDiagnosticoIniciado || r.saidas[1].Tipo != TipoDiagnosticoConcluido {
		t.Fatalf("saídas = %+v; esperava DiagnosticoIniciado e DiagnosticoConcluido, nessa ordem", r.saidas)
	}
	concl := r.saidas[1]
	if concl.SagaID != cmd.SagaID || concl.OsID != cmd.OsID || concl.Payload.(map[string]string)["diagnostico"] != "troca do filtro" {
		t.Errorf("DiagnosticoConcluido incompleto: %+v", concl)
	}
}

func TestConcluirDiagnostico_Erros(t *testing.T) {
	r := novoRepo()
	s := servico(r)
	if _, err := s.ConcluirDiagnostico(context.Background(), uuid.New(), "mec", "x"); !errors.Is(err, ErrNaoEncontrada) {
		t.Errorf("tarefa inexistente: %v", err)
	}
	_ = s.IniciarDiagnostico(context.Background(), comando(), payload("ABC1D23"))
	id := unica(t, r).ID
	if _, err := s.ConcluirDiagnostico(context.Background(), id, "mec", ""); !errors.Is(err, tarefa.ErrDiagnosticoVazio) {
		t.Errorf("diagnóstico vazio: %v", err)
	}
	r.errEscrita = ErrConflito
	if _, err := s.ConcluirDiagnostico(context.Background(), id, "mec", "x"); !errors.Is(err, ErrConflito) {
		t.Errorf("conflito do repositório não propagado: %v", err)
	}
}

func TestIniciar_Erros(t *testing.T) {
	r := novoRepo()
	s := servico(r)
	if _, err := s.Iniciar(context.Background(), uuid.New(), "mec"); !errors.Is(err, ErrNaoEncontrada) {
		t.Errorf("inexistente: %v", err)
	}
	_ = s.IniciarDiagnostico(context.Background(), comando(), payload("ABC1D23"))
	id := unica(t, r).ID
	if _, err := s.Iniciar(context.Background(), id, ""); !errors.Is(err, tarefa.ErrMecanicoObrigatorio) {
		t.Errorf("sem mecânico: %v", err)
	}
	r.errEscrita = errors.New("dynamodb fora")
	if _, err := s.Iniciar(context.Background(), id, "mec"); err == nil {
		t.Error("erro de escrita não propagado")
	}
}

// Dois mecânicos iniciando a mesma tarefa: o segundo recebe conflito.
func TestIniciar_ConcorrenciaViraConflito(t *testing.T) {
	r := novoRepo()
	s := servico(r)
	_ = s.IniciarDiagnostico(context.Background(), comando(), payload("ABC1D23"))
	id := unica(t, r).ID
	if _, err := s.Iniciar(context.Background(), id, "mec-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Iniciar(context.Background(), id, "mec-2"); !errors.Is(err, tarefa.ErrTransicaoInvalida) {
		t.Errorf("segundo início: %v", err)
	}
}

func TestListarFila_OrdemDeAtendimento(t *testing.T) {
	r := novoRepo()
	base := relogio
	add := func(tipo tarefa.Tipo, minutos int) uuid.UUID {
		tf, _ := tarefa.Nova(tipo, uuid.New(), uuid.New(), "", "AAA0000", "", "", base.Add(time.Duration(minutos)*time.Minute))
		r.tarefas[tf.ID] = tf
		return tf.ID
	}
	diagAntigo := add(tarefa.Diagnostico, 0)
	reparoNovo := add(tarefa.Reparo, 30)
	diagNovo := add(tarefa.Diagnostico, 10)
	reparoAntigo := add(tarefa.Reparo, 5)

	fila, err := servico(r).ListarFila(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	esperado := []uuid.UUID{reparoAntigo, reparoNovo, diagAntigo, diagNovo}
	for i, id := range esperado {
		if fila[i].ID != id {
			t.Fatalf("posição %d: %s %v; esperava reparos (por chegada) antes de diagnósticos", i, fila[i].Tipo, fila[i].CriadoEm)
		}
	}

	so, _ := servico(r).ListarFila(context.Background(), tarefa.Reparo)
	if len(so) != 2 {
		t.Errorf("filtro por tipo: %d", len(so))
	}
	if _, err := servico(r).ListarFila(context.Background(), "PINTURA"); !errors.Is(err, ErrTipoInvalido) {
		t.Errorf("tipo inválido: %v", err)
	}
	r.errLeitura = errors.New("fora")
	if _, err := servico(r).ListarFila(context.Background(), ""); err == nil {
		t.Error("erro de leitura não propagado")
	}
}

func TestListarPorOS(t *testing.T) {
	r := novoRepo()
	s := servico(r)
	cmd := comando()
	_ = s.IniciarDiagnostico(context.Background(), cmd, payload("ABC1D23"))
	_ = s.IniciarDiagnostico(context.Background(), comando(), payload("XYZ9Z99"))

	ts, err := s.ListarPorOS(context.Background(), cmd.OsID)
	if err != nil || len(ts) != 1 || ts[0].OsID != cmd.OsID {
		t.Errorf("ListarPorOS = %v, %v", ts, err)
	}
}

// ── Reparo e cancelamento ─────────────────────────────────────────────────────

func payloadReparo(placa string) PayloadExecutarReparo {
	var p PayloadExecutarReparo
	p.NumeroOS, p.Veiculo.Placa, p.Veiculo.Modelo, p.Diagnostico = "OS-1", placa, "Gol", "amortecedor vazando"
	return p
}

func TestExecutarReparo_CriaTarefaDeReparo(t *testing.T) {
	r := novoRepo()
	cmd := comando()
	cmd.Tipo = TipoExecutarReparo
	if err := servico(r).ExecutarReparo(context.Background(), cmd, payloadReparo("ABC1D23")); err != nil {
		t.Fatal(err)
	}
	tf := unica(t, r)
	if tf.Tipo != tarefa.Reparo || tf.Estado != tarefa.Aguardando || tf.Descricao != "amortecedor vazando" || len(r.saidas) != 0 {
		t.Errorf("reparo = %+v, saídas %v", tf, r.saidas)
	}
}

func TestExecutarReparo_FalhaSimulada(t *testing.T) {
	ligada := Novo(novoRepo(), ComFalhaSimulada(true))
	ligada.agora = func() time.Time { return relogio }
	r := ligada.repo.(*repoMemoria)
	if err := ligada.ExecutarReparo(context.Background(), comando(), payloadReparo("FAL1A23")); err != nil {
		t.Fatal(err)
	}
	if len(r.tarefas) != 0 || len(r.saidas) != 1 || r.saidas[0].Tipo != TipoExecucaoFalhou ||
		r.saidas[0].Payload.(map[string]string)["motivo"] != "falha_simulada" {
		t.Fatalf("FAL* com a opção ligada deve responder ExecucaoFalhou: %+v", r.saidas)
	}

	// Desligada (padrão): placa FAL* é um carro como outro qualquer.
	d := novoRepo()
	if err := servico(d).ExecutarReparo(context.Background(), comando(), payloadReparo("FAL1A23")); err != nil || len(d.tarefas) != 1 {
		t.Errorf("sem a opção, FAL* segue o fluxo normal: %v %d", err, len(d.tarefas))
	}
}

func TestExecutarReparo_SemPlacaEErros(t *testing.T) {
	r := novoRepo()
	if err := servico(r).ExecutarReparo(context.Background(), comando(), payloadReparo(" ")); err != nil || r.saidas[0].Tipo != TipoExecucaoFalhou {
		t.Errorf("sem placa: %v %v", err, r.saidas)
	}
	e := novoRepo()
	e.errEscrita = errors.New("dynamodb fora")
	if err := servico(e).ExecutarReparo(context.Background(), comando(), payloadReparo("ABC1D23")); err == nil {
		t.Error("erro de infra deve voltar para a fila")
	}
}

func TestReparo_IniciarEConcluirRespondemASaga(t *testing.T) {
	r := novoRepo()
	s := servico(r)
	cmd := comando()
	_ = s.ExecutarReparo(context.Background(), cmd, payloadReparo("ABC1D23"))
	id := unica(t, r).ID

	if _, err := s.ConcluirReparo(context.Background(), id); !errors.Is(err, tarefa.ErrTransicaoInvalida) {
		t.Errorf("concluir sem iniciar (pivô): %v", err)
	}
	if _, err := s.Iniciar(context.Background(), id, "mec-1"); err != nil {
		t.Fatal(err)
	}
	tf, err := s.ConcluirReparo(context.Background(), id)
	if err != nil || tf.Estado != tarefa.Concluida {
		t.Fatalf("concluir: %v %+v", err, tf)
	}
	if len(r.saidas) != 2 || r.saidas[0].Tipo != TipoExecucaoIniciada || r.saidas[1].Tipo != TipoExecucaoConcluida ||
		r.saidas[1].OsID != cmd.OsID || r.saidas[0].Payload.(map[string]string)["iniciado_em"] == "" {
		t.Errorf("saídas = %+v", r.saidas)
	}
	if _, err := s.ConcluirReparo(context.Background(), uuid.New()); !errors.Is(err, ErrNaoEncontrada) {
		t.Errorf("inexistente: %v", err)
	}
	r.errEscrita = ErrConflito
	r.tarefas[id] = func() tarefa.Tarefa { x := r.tarefas[id]; x.Estado = tarefa.EmAndamento; return x }()
	if _, err := s.ConcluirReparo(context.Background(), id); !errors.Is(err, ErrConflito) {
		t.Errorf("conflito: %v", err)
	}
}

func TestCancelarTarefas(t *testing.T) {
	r := novoRepo()
	s := servico(r)
	cmd := comando()
	_ = s.IniciarDiagnostico(context.Background(), cmd, payload("ABC1D23")) // aguardando
	rep := cmd
	rep.MessageID = uuid.New()
	_ = s.ExecutarReparo(context.Background(), rep, payloadReparo("ABC1D23")) // aguardando
	var diag uuid.UUID
	for id, tf := range r.tarefas {
		if tf.Tipo == tarefa.Diagnostico {
			diag = id
		}
	}
	_, _ = s.ConcluirDiagnostico(context.Background(), diag, "mec", "x") // concluída: fica como está
	r.saidas = nil

	cancelar := Comando{MessageID: uuid.New(), Tipo: TipoCancelarTarefas, SagaID: cmd.SagaID, OsID: cmd.OsID}
	if err := s.CancelarTarefas(context.Background(), cancelar); err != nil {
		t.Fatal(err)
	}
	if r.saidas[0].Tipo != TipoTarefasCanceladas || r.saidas[0].Payload.(map[string]int)["quantidade"] != 1 {
		t.Fatalf("resposta = %+v", r.saidas)
	}
	if r.tarefas[diag].Estado != tarefa.Concluida {
		t.Error("tarefa concluída não pode ser cancelada")
	}
	if err := s.CancelarTarefas(context.Background(), cancelar); !errors.Is(err, ErrJaProcessada) {
		t.Errorf("reentrega: %v", err)
	}
}

func TestCancelarTarefas_SemTarefasRespondeZero(t *testing.T) {
	r := novoRepo()
	if err := servico(r).CancelarTarefas(context.Background(), comando()); err != nil {
		t.Fatal(err)
	}
	if r.saidas[0].Payload.(map[string]int)["quantidade"] != 0 {
		t.Error("zero é resposta válida (o orquestrador segue a compensação)")
	}
	e := novoRepo()
	e.errLeitura = errors.New("fora")
	if err := servico(e).CancelarTarefas(context.Background(), comando()); err == nil {
		t.Error("erro de leitura deve voltar para a fila")
	}
}
