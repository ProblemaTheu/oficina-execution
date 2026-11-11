package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/application/execucao"
	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
	"github.com/ProblemaTheu/oficina-execution/internal/domain/tarefa"
)

var segredo = []byte("segredo-de-teste-com-mais-de-32-caracteres")

func token(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	if _, ok := claims["aud"]; !ok {
		claims["aud"] = audiencia
	}
	claims["exp"] = time.Now().Add(time.Hour).Unix()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(segredo)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mecanicoToken(t *testing.T) string {
	return token(t, jwt.MapClaims{"sub": "mec-1", "tipo": "usuario", "papel": "mecanico"})
}

// ucFake registra as chamadas e devolve respostas programadas.
type ucFake struct {
	tarefa      tarefa.Tarefa
	err         error
	mecanico    string
	diagnostico string
	tipoFila    tarefa.Tipo
	correlacao  string
}

func (u *ucFake) ListarFila(_ context.Context, tipo tarefa.Tipo) ([]tarefa.Tarefa, error) {
	u.tipoFila = tipo
	return []tarefa.Tarefa{u.tarefa}, u.err
}
func (u *ucFake) ListarPorOS(_ context.Context, _ uuid.UUID) ([]tarefa.Tarefa, error) {
	return []tarefa.Tarefa{u.tarefa}, u.err
}
func (u *ucFake) Iniciar(ctx context.Context, _ uuid.UUID, mec string) (tarefa.Tarefa, error) {
	u.mecanico, u.correlacao = mec, logging.Correlacao(ctx)
	return u.tarefa, u.err
}
func (u *ucFake) ConcluirDiagnostico(_ context.Context, _ uuid.UUID, mec, diag string) (tarefa.Tarefa, error) {
	u.mecanico, u.diagnostico = mec, diag
	return u.tarefa, u.err
}

func tarefaFake() tarefa.Tarefa {
	tf, _ := tarefa.Nova(tarefa.Diagnostico, uuid.New(), uuid.New(), "OS-2026-00001", "ABC1D23", "Gol", "", time.Now())
	return tf
}

func chamar(t *testing.T, h http.Handler, metodo, caminho, tok, corpo string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(metodo, caminho, strings.NewReader(corpo))
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("X-Correlation-Id", "req-http")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func pronto(error) Prontidao { return func(context.Context) error { return nil } }

func TestHealth(t *testing.T) {
	h := Rotas(&ucFake{}, segredo, pronto(nil))
	if rec := chamar(t, h, "GET", "/health/live", "", ""); rec.Code != 200 {
		t.Errorf("live = %d", rec.Code)
	}
	if rec := chamar(t, h, "GET", "/health/ready", "", ""); rec.Code != 200 {
		t.Errorf("ready = %d", rec.Code)
	}
	fora := Rotas(&ucFake{}, segredo, func(context.Context) error { return errors.New("dynamodb fora") })
	if rec := chamar(t, fora, "GET", "/health/ready", "", ""); rec.Code != 503 {
		t.Errorf("ready com dependência fora = %d, esperava 503", rec.Code)
	}
}

func TestAutenticacao(t *testing.T) {
	h := Rotas(&ucFake{tarefa: tarefaFake()}, segredo, pronto(nil))
	casos := map[string]struct {
		tok    string
		status int
	}{
		"sem token":           {"", 401},
		"token inválido":      {"abc.def.ghi", 401},
		"audiência errada":    {token(t, jwt.MapClaims{"sub": "x", "aud": "outro-sistema"}), 401},
		"cliente não vê fila": {token(t, jwt.MapClaims{"sub": "c1", "tipo": "cliente"}), 403},
		"atendente vê fila":   {token(t, jwt.MapClaims{"sub": "a1", "tipo": "usuario", "papel": "atendente"}), 200},
	}
	for nome, c := range casos {
		t.Run(nome, func(t *testing.T) {
			if rec := chamar(t, h, "GET", "/execution/v1/fila", c.tok, ""); rec.Code != c.status {
				t.Errorf("status = %d, esperava %d (%s)", rec.Code, c.status, rec.Body)
			}
		})
	}
}

func TestFila_RepassaTipoEDevolveLista(t *testing.T) {
	uc := &ucFake{tarefa: tarefaFake()}
	rec := chamar(t, Rotas(uc, segredo, pronto(nil)), "GET", "/execution/v1/fila?tipo=DIAGNOSTICO", mecanicoToken(t), "")

	var corpo []tarefaJSON
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &corpo) != nil || len(corpo) != 1 || corpo[0].Placa != "ABC1D23" {
		t.Fatalf("fila = %d %s", rec.Code, rec.Body)
	}
	if uc.tipoFila != tarefa.Diagnostico {
		t.Errorf("tipo repassado = %q", uc.tipoFila)
	}
	if rec.Header().Get("X-Correlation-Id") != "req-http" {
		t.Error("correlation id não devolvido")
	}
}

func TestTarefasDaOS(t *testing.T) {
	h := Rotas(&ucFake{tarefa: tarefaFake()}, segredo, pronto(nil))
	if rec := chamar(t, h, "GET", "/execution/v1/tarefas?os_id="+uuid.NewString(), mecanicoToken(t), ""); rec.Code != 200 {
		t.Errorf("status = %d", rec.Code)
	}
	if rec := chamar(t, h, "GET", "/execution/v1/tarefas?os_id=x", mecanicoToken(t), ""); rec.Code != 400 {
		t.Errorf("os_id inválido = %d", rec.Code)
	}
	cli := token(t, jwt.MapClaims{"sub": "c1", "tipo": "cliente"})
	if rec := chamar(t, h, "GET", "/execution/v1/tarefas?os_id="+uuid.NewString(), cli, ""); rec.Code != 403 {
		t.Errorf("cliente = %d", rec.Code)
	}
}

func TestIniciarEConcluir_UsamOMecanicoDoToken(t *testing.T) {
	uc := &ucFake{tarefa: tarefaFake()}
	h := Rotas(uc, segredo, pronto(nil))
	id := uuid.NewString()

	if rec := chamar(t, h, "POST", "/execution/v1/tarefas/"+id+"/iniciar", mecanicoToken(t), ""); rec.Code != 200 {
		t.Fatalf("iniciar = %d %s", rec.Code, rec.Body)
	}
	if uc.mecanico != "mec-1" || uc.correlacao != "req-http" {
		t.Errorf("mecânico/correlação não repassados: %q %q", uc.mecanico, uc.correlacao)
	}
	rec := chamar(t, h, "POST", "/execution/v1/tarefas/"+id+"/concluir-diagnostico", mecanicoToken(t), `{"diagnostico":"troca do filtro"}`)
	if rec.Code != 200 || uc.diagnostico != "troca do filtro" {
		t.Fatalf("concluir = %d %s (diag %q)", rec.Code, rec.Body, uc.diagnostico)
	}
}

func TestMutacoes_Validacoes(t *testing.T) {
	h := Rotas(&ucFake{tarefa: tarefaFake()}, segredo, pronto(nil))
	atendente := token(t, jwt.MapClaims{"sub": "a1", "tipo": "usuario", "papel": "atendente"})
	if rec := chamar(t, h, "POST", "/execution/v1/tarefas/"+uuid.NewString()+"/iniciar", atendente, ""); rec.Code != 403 {
		t.Errorf("atendente iniciando = %d, esperava 403", rec.Code)
	}
	if rec := chamar(t, h, "POST", "/execution/v1/tarefas/nao-e-uuid/iniciar", mecanicoToken(t), ""); rec.Code != 400 {
		t.Errorf("id inválido = %d", rec.Code)
	}
	if rec := chamar(t, h, "POST", "/execution/v1/tarefas/"+uuid.NewString()+"/concluir-diagnostico", mecanicoToken(t), `{`); rec.Code != 400 {
		t.Errorf("corpo inválido = %d", rec.Code)
	}
	if rec := chamar(t, h, "POST", "/execution/v1/tarefas/nao-e-uuid/concluir-diagnostico", mecanicoToken(t), `{}`); rec.Code != 400 {
		t.Errorf("id inválido no concluir = %d", rec.Code)
	}
}

func TestErros_TraduzidosParaHTTP(t *testing.T) {
	casos := []struct {
		err    error
		status int
		codigo string
	}{
		{execucao.ErrNaoEncontrada, 404, "NOT_FOUND"},
		{tarefa.ErrTarefaCancelada, 409, "TAREFA_CANCELADA"},
		{execucao.ErrConflito, 409, "CONFLITO"},
		{tarefa.ErrTransicaoInvalida, 409, "CONFLITO"},
		{tarefa.ErrDiagnosticoVazio, 422, "REGRA_DE_NEGOCIO"},
		{execucao.ErrTipoInvalido, 422, "REGRA_DE_NEGOCIO"},
		{errors.New("dynamodb fora"), 500, "ERRO_INTERNO"},
	}
	for _, c := range casos {
		t.Run(c.codigo+"/"+c.err.Error(), func(t *testing.T) {
			h := Rotas(&ucFake{err: c.err}, segredo, pronto(nil))
			for _, rota := range []struct{ metodo, caminho, corpo string }{
				{"POST", "/execution/v1/tarefas/" + uuid.NewString() + "/iniciar", ""},
				{"POST", "/execution/v1/tarefas/" + uuid.NewString() + "/concluir-diagnostico", `{"diagnostico":"x"}`},
				{"GET", "/execution/v1/fila", ""},
				{"GET", "/execution/v1/tarefas?os_id=" + uuid.NewString(), ""},
			} {
				rec := chamar(t, h, rota.metodo, rota.caminho, mecanicoToken(t), rota.corpo)
				var corpo map[string]string
				_ = json.Unmarshal(rec.Body.Bytes(), &corpo)
				if rec.Code != c.status || corpo["code"] != c.codigo {
					t.Errorf("%s %s: %d %s, esperava %d %s", rota.metodo, rota.caminho, rec.Code, corpo["code"], c.status, c.codigo)
				}
			}
		})
	}
}

func TestSegredoJWT(t *testing.T) {
	t.Setenv("JWT_SECRET", "curto")
	if _, err := SegredoJWT(); err == nil {
		t.Error("segredo curto deve falhar")
	}
	t.Setenv("JWT_SECRET", string(segredo))
	if s, err := SegredoJWT(); err != nil || string(s) != string(segredo) {
		t.Errorf("SegredoJWT = %q, %v", s, err)
	}
}
