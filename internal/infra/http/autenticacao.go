// Package http expõe a API do mecânico (/execution/v1) e os probes de saúde.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// O token é o mesmo do OS Service (contrato F3-0.2): HS256 com o segredo
// compartilhado, aud=oficina-api, tipo=usuario|cliente e papel.
const (
	audiencia    = "oficina-api"
	tipoUsuario  = "usuario"
	tamanhoChave = 32
)

type ctxClaims struct{}

// SegredoJWT lê JWT_SECRET e falha alto se ausente ou curto — mesmo
// comportamento do OS Service: melhor não subir do que subir inseguro.
func SegredoJWT() ([]byte, error) {
	v := os.Getenv("JWT_SECRET")
	if len(v) < tamanhoChave {
		return nil, fmt.Errorf("JWT_SECRET ausente ou com menos de %d caracteres", tamanhoChave)
	}
	return []byte(v), nil
}

// autenticar valida o Bearer token e guarda os claims no contexto.
func autenticar(segredo []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cabecalho := r.Header.Get("Authorization")
			if !strings.HasPrefix(cabecalho, "Bearer ") {
				erroJSON(w, http.StatusUnauthorized, "nao_autorizado", "token ausente ou mal formatado")
				return
			}
			token, err := jwt.Parse(strings.TrimPrefix(cabecalho, "Bearer "), func(*jwt.Token) (any, error) {
				return segredo, nil
			}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithAudience(audiencia))
			if err != nil || !token.Valid {
				erroJSON(w, http.StatusUnauthorized, "nao_autorizado", "token inválido ou expirado")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxClaims{}, token.Claims.(jwt.MapClaims))))
		})
	}
}

func claim(ctx context.Context, nome string) string {
	c, _ := ctx.Value(ctxClaims{}).(jwt.MapClaims)
	v, _ := c[nome].(string)
	return v
}

// funcionario: só tokens de funcionário (cliente não vê a fila da oficina).
// Token sem "tipo" é de funcionário — mesma regra de transição do OS.
func funcionario(ctx context.Context) bool {
	t := claim(ctx, "tipo")
	return t == "" || t == tipoUsuario
}

// mecanico: quem pode mexer em tarefa.
func mecanico(ctx context.Context) bool {
	p := claim(ctx, "papel")
	return funcionario(ctx) && (p == "mecanico" || p == "administrador")
}

func erroJSON(w http.ResponseWriter, status int, codigo, mensagem string) {
	escreverJSON(w, status, map[string]string{"code": codigo, "message": mensagem})
}

func escreverJSON(w http.ResponseWriter, status int, corpo any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(corpo)
}
