// Execution Service: fila de diagnóstico e reparo da oficina (Fase 4).
//
// Um processo com três laços: API do mecânico (HTTP), consumidor da fila
// execution-commands.fifo e relay da outbox para os-saga-replies.fifo.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/newrelic/go-agent/v3/integrations/logcontext-v2/nrslog"
	"github.com/newrelic/go-agent/v3/newrelic"

	"github.com/ProblemaTheu/oficina-execution/internal/application/execucao"
	"github.com/ProblemaTheu/oficina-execution/internal/infra/comandos"
	"github.com/ProblemaTheu/oficina-execution/internal/infra/dynamo"
	apihttp "github.com/ProblemaTheu/oficina-execution/internal/infra/http"
	"github.com/ProblemaTheu/oficina-execution/internal/infra/mensageria"
)

// configuracao vem do ambiente (ConfigMap/Secret no EKS, compose no local).
type configuracao struct {
	Tabela          string // DYNAMODB_TABELA
	FilaComandos    string // FILA_EXECUTION_URL — de onde consome
	FilaRespostas   string // FILA_RESPOSTAS_URL — para onde responde
	Porta           string // PORTA (padrão 8080)
	SegredoJWT      []byte
	DesligarEsperas time.Duration
}

func lerConfiguracao() (configuracao, error) {
	c := configuracao{
		Tabela:          os.Getenv("DYNAMODB_TABELA"),
		FilaComandos:    os.Getenv("FILA_EXECUTION_URL"),
		FilaRespostas:   os.Getenv("FILA_RESPOSTAS_URL"),
		Porta:           os.Getenv("PORTA"),
		DesligarEsperas: 30 * time.Second,
	}
	if c.Porta == "" {
		c.Porta = "8080"
	}
	var faltando []string
	for nome, v := range map[string]string{"DYNAMODB_TABELA": c.Tabela, "FILA_EXECUTION_URL": c.FilaComandos, "FILA_RESPOSTAS_URL": c.FilaRespostas} {
		if v == "" {
			faltando = append(faltando, nome)
		}
	}
	if len(faltando) > 0 {
		return c, fmt.Errorf("variáveis obrigatórias ausentes: %v", faltando)
	}
	segredo, err := apihttp.SegredoJWT()
	if err != nil {
		return c, err
	}
	c.SegredoJWT = segredo
	return c, nil
}

func main() {
	nrApp := novoAppNewRelic()
	configurarLog(nrApp)

	cfg, err := lerConfiguracao()
	if err != nil {
		slog.Error("configuração inválida", "error", err)
		os.Exit(1)
	}

	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error("configuração AWS", "error", err)
		os.Exit(1)
	}
	tracer := mensageria.TracerNewRelic{App: nrApp}
	repo := dynamo.Novo(dynamodb.NewFromConfig(awsCfg), cfg.Tabela, tracer)
	svc := execucao.Novo(repo, execucao.ComFalhaSimulada(os.Getenv("PERMITIR_FALHA_SIMULADA") == "true"))
	clienteSQS := sqs.NewFromConfig(awsCfg)

	consumidor := mensageria.NovoConsumer(clienteSQS, mensageria.ConfigConsumer{
		FilaURL: cfg.FilaComandos, Handlers: comandos.Handlers(svc), Tracer: tracer,
	})
	relay := mensageria.NovoRelay(repo, mensageria.NovoPublisher(clienteSQS), mensageria.ConfigRelay{
		Destinos: map[string]string{dynamo.DestinoOS: cfg.FilaRespostas},
	})

	// Prontidão: uma leitura na tabela prova credencial, rede e tabela.
	pronto := func(ctx context.Context) error {
		_, err := repo.Buscar(ctx, uuid.Nil)
		if errors.Is(err, execucao.ErrNaoEncontrada) {
			return nil
		}
		return err
	}
	srv := &http.Server{
		Addr:              ":" + cfg.Porta,
		Handler:           apihttp.Rotas(svc, cfg.SegredoJWT, pronto),
		ReadHeaderTimeout: 10 * time.Second,
	}

	var wg sync.WaitGroup
	for nome, laco := range map[string]func(context.Context) error{"consumidor": consumidor.Executar, "relay": relay.Executar} {
		wg.Add(1)
		go func(nome string, laco func(context.Context) error) {
			defer wg.Done()
			if err := laco(ctx); err != nil {
				slog.Error(nome+" parou", "error", err)
			}
		}(nome, laco)
	}
	go func() {
		slog.Info("execution service no ar", "porta", cfg.Porta, "tabela", cfg.Tabela, "fila_comandos", cfg.FilaComandos)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("servidor HTTP", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("sinal de término recebido; encerrando")
	desligar, cancelar := context.WithTimeout(context.Background(), cfg.DesligarEsperas)
	defer cancelar()
	if err := srv.Shutdown(desligar); err != nil {
		slog.Warn("shutdown HTTP com erro", "error", err)
	}
	wg.Wait() // consumidor termina a mensagem em curso; relay termina o envio
	slog.Info("encerrado")
}

// novoAppNewRelic liga o APM quando há licença; sem ela o serviço roda normal.
func novoAppNewRelic() *newrelic.Application {
	licenca := os.Getenv("NEW_RELIC_LICENSE_KEY")
	if licenca == "" {
		slog.Info("new relic desabilitado: NEW_RELIC_LICENSE_KEY ausente")
		return nil
	}
	nome := os.Getenv("NEW_RELIC_APP_NAME")
	if nome == "" {
		nome = "oficina-execution"
	}
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName(nome), newrelic.ConfigLicense(licenca),
		newrelic.ConfigDistributedTracerEnabled(true), newrelic.ConfigAppLogForwardingEnabled(true),
	)
	if err != nil {
		slog.Warn("new relic: falha ao inicializar", "error", err)
		return nil
	}
	return app
}

// configurarLog: JSON com os nomes de campo que o New Relic reconhece (mesmo
// formato do OS Service, F3-4.1).
func configurarLog(nrApp *newrelic.Application) {
	nivel := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		nivel = slog.LevelDebug
	}
	var h slog.Handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: nivel,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Key = "timestamp"
			case slog.MessageKey:
				a.Key = "message"
			}
			return a
		},
	})
	if nrApp != nil {
		h = nrslog.WrapHandler(nrApp, h)
	}
	nome := os.Getenv("NEW_RELIC_APP_NAME")
	if nome == "" {
		nome = "oficina-execution"
	}
	slog.SetDefault(slog.New(h).With("service.name", nome, "hostname", os.Getenv("HOSTNAME")))
}
