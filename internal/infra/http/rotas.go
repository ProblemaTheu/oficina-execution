package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/application/execucao"
	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
	"github.com/ProblemaTheu/oficina-execution/internal/domain/tarefa"
)

// casosDeUso é o que a API usa do execucao.Servico.
type casosDeUso interface {
	ListarFila(ctx context.Context, tipo tarefa.Tipo) ([]tarefa.Tarefa, error)
	ListarPorOS(ctx context.Context, osID uuid.UUID) ([]tarefa.Tarefa, error)
	Iniciar(ctx context.Context, id uuid.UUID, mecanico string) (tarefa.Tarefa, error)
	ConcluirDiagnostico(ctx context.Context, id uuid.UUID, mecanico, diagnostico string) (tarefa.Tarefa, error)
}

// Prontidao verifica as dependências para o /health/ready.
type Prontidao func(ctx context.Context) error

// Rotas monta o roteador: probes públicos e /execution/v1 com JWT.
func Rotas(uc casosDeUso, segredo []byte, pronto Prontidao) http.Handler {
	r := chi.NewRouter()
	r.Use(correlacao)
	r.Get("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		escreverJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	})
	r.Get("/health/ready", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancelar := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancelar()
		if err := pronto(ctx); err != nil {
			logging.Log(ctx).Warn("health: dependência indisponível", "error", err)
			escreverJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "DOWN", "components": map[string]string{"dynamodb": "DOWN"}})
			return
		}
		escreverJSON(w, http.StatusOK, map[string]any{"status": "UP", "components": map[string]string{"dynamodb": "UP"}})
	})

	h := handlers{uc: uc}
	r.Route("/execution/v1", func(r chi.Router) {
		r.Use(autenticar(segredo))
		r.Get("/fila", h.fila)
		r.Get("/tarefas", h.tarefasDaOS)
		r.Post("/tarefas/{id}/iniciar", h.iniciar)
		r.Post("/tarefas/{id}/concluir-diagnostico", h.concluirDiagnostico)
	})
	return r
}

type handlers struct{ uc casosDeUso }

// tarefaJSON é a representação da tarefa na API.
type tarefaJSON struct {
	ID           uuid.UUID  `json:"id"`
	OsID         uuid.UUID  `json:"os_id"`
	NumeroOS     string     `json:"numero_os,omitempty"`
	Tipo         string     `json:"tipo"`
	Estado       string     `json:"estado"`
	Placa        string     `json:"placa,omitempty"`
	Modelo       string     `json:"modelo,omitempty"`
	Descricao    string     `json:"descricao,omitempty"`
	Mecanico     string     `json:"mecanico,omitempty"`
	Diagnostico  string     `json:"diagnostico,omitempty"`
	CriadoEm     time.Time  `json:"criado_em"`
	IniciadoEm   *time.Time `json:"iniciado_em,omitempty"`
	FinalizadoEm *time.Time `json:"finalizado_em,omitempty"`
}

func paraJSON(t tarefa.Tarefa) tarefaJSON {
	return tarefaJSON{ID: t.ID, OsID: t.OsID, NumeroOS: t.NumeroOS, Tipo: string(t.Tipo), Estado: string(t.Estado),
		Placa: t.Placa, Modelo: t.Modelo, Descricao: t.Descricao, Mecanico: t.Mecanico, Diagnostico: t.Diagnostico,
		CriadoEm: t.CriadoEm, IniciadoEm: t.IniciadoEm, FinalizadoEm: t.FinalizadoEm}
}

func lista(ts []tarefa.Tarefa) []tarefaJSON {
	out := make([]tarefaJSON, 0, len(ts))
	for _, t := range ts {
		out = append(out, paraJSON(t))
	}
	return out
}

func (h handlers) fila(w http.ResponseWriter, r *http.Request) {
	if !funcionario(r.Context()) {
		erroJSON(w, http.StatusForbidden, "ACCESS_DENIED", "a fila da oficina é restrita a funcionários")
		return
	}
	ts, err := h.uc.ListarFila(r.Context(), tarefa.Tipo(r.URL.Query().Get("tipo")))
	if err != nil {
		responderErro(w, r, err)
		return
	}
	escreverJSON(w, http.StatusOK, lista(ts))
}

func (h handlers) tarefasDaOS(w http.ResponseWriter, r *http.Request) {
	if !funcionario(r.Context()) {
		erroJSON(w, http.StatusForbidden, "ACCESS_DENIED", "restrito a funcionários")
		return
	}
	osID, err := uuid.Parse(r.URL.Query().Get("os_id"))
	if err != nil {
		erroJSON(w, http.StatusBadRequest, "VALIDATION_ERROR", "os_id obrigatório (uuid)")
		return
	}
	ts, err := h.uc.ListarPorOS(r.Context(), osID)
	if err != nil {
		responderErro(w, r, err)
		return
	}
	escreverJSON(w, http.StatusOK, lista(ts))
}

// idDaTarefa valida o papel e o id do caminho; escreve o erro e devolve false.
func idDaTarefa(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	if !mecanico(r.Context()) {
		erroJSON(w, http.StatusForbidden, "ACCESS_DENIED", "operação restrita a mecânicos e administradores")
		return uuid.Nil, false
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		erroJSON(w, http.StatusBadRequest, "VALIDATION_ERROR", "id da tarefa inválido")
		return uuid.Nil, false
	}
	return id, true
}

func (h handlers) iniciar(w http.ResponseWriter, r *http.Request) {
	id, ok := idDaTarefa(w, r)
	if !ok {
		return
	}
	t, err := h.uc.Iniciar(r.Context(), id, claim(r.Context(), "sub"))
	if err != nil {
		responderErro(w, r, err)
		return
	}
	escreverJSON(w, http.StatusOK, paraJSON(t))
}

func (h handlers) concluirDiagnostico(w http.ResponseWriter, r *http.Request) {
	id, ok := idDaTarefa(w, r)
	if !ok {
		return
	}
	var corpo struct {
		Diagnostico string `json:"diagnostico"`
	}
	if err := json.NewDecoder(r.Body).Decode(&corpo); err != nil {
		erroJSON(w, http.StatusBadRequest, "VALIDATION_ERROR", "corpo JSON inválido")
		return
	}
	t, err := h.uc.ConcluirDiagnostico(r.Context(), id, claim(r.Context(), "sub"), corpo.Diagnostico)
	if err != nil {
		responderErro(w, r, err)
		return
	}
	escreverJSON(w, http.StatusOK, paraJSON(t))
}

// responderErro traduz erros de domínio/aplicação em HTTP.
func responderErro(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, execucao.ErrNaoEncontrada):
		erroJSON(w, http.StatusNotFound, "NOT_FOUND", "tarefa não encontrada")
	case errors.Is(err, tarefa.ErrTarefaCancelada):
		erroJSON(w, http.StatusConflict, "TAREFA_CANCELADA", err.Error())
	case errors.Is(err, execucao.ErrConflito), errors.Is(err, tarefa.ErrTransicaoInvalida):
		erroJSON(w, http.StatusConflict, "CONFLITO", err.Error())
	case errors.Is(err, tarefa.ErrDiagnosticoVazio), errors.Is(err, tarefa.ErrTipoErrado),
		errors.Is(err, tarefa.ErrMecanicoObrigatorio), errors.Is(err, execucao.ErrTipoInvalido):
		erroJSON(w, http.StatusUnprocessableEntity, "REGRA_DE_NEGOCIO", err.Error())
	default:
		logging.Log(r.Context()).Error("erro inesperado", "error", err)
		erroJSON(w, http.StatusInternalServerError, "ERRO_INTERNO", "erro inesperado")
	}
}

// cabecalhoCorrelacao é o mesmo header do OS Service.
const cabecalhoCorrelacao = "X-Correlation-Id"

// correlacao reaproveita (ou cria) o X-Correlation-Id e o coloca no logger e
// no contexto — as respostas à saga gravadas por esta requisição levam o id.
func correlacao(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(cabecalhoCorrelacao)
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(cabecalhoCorrelacao, id)
		log := logging.Log(r.Context()).With("correlation_id", id, "http.method", r.Method, "http.route", r.URL.Path)
		ctx := logging.ComCorrelacao(logging.ComLogger(r.Context(), log), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
