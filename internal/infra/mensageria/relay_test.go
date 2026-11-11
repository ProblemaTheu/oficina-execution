package mensageria

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// outboxFake é uma outbox em memória que imita o contrato do adaptador Postgres.
type outboxFake struct {
	mu         sync.Mutex
	msgs       []*MensagemOutbox
	enviada    map[uuid.UUID]bool
	desistida  map[uuid.UUID]bool
	liderOcup  bool // outra réplica segura a liderança
	liberacoes int

	erroLider, erroPendentes, erroMarcar, erroFalha error
}

func novaOutbox(msgs ...MensagemOutbox) *outboxFake {
	o := &outboxFake{enviada: map[uuid.UUID]bool{}, desistida: map[uuid.UUID]bool{}}
	for i := range msgs {
		m := msgs[i]
		o.msgs = append(o.msgs, &m)
	}
	return o
}

func (o *outboxFake) TentarLideranca(context.Context) (func(), bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.erroLider != nil {
		return nil, false, o.erroLider
	}
	if o.liderOcup {
		return nil, false, nil
	}
	o.liderOcup = true
	return func() {
		o.mu.Lock()
		o.liderOcup = false
		o.liberacoes++
		o.mu.Unlock()
	}, true, nil
}

func (o *outboxFake) Pendentes(_ context.Context, limite int) ([]MensagemOutbox, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.erroPendentes != nil {
		return nil, o.erroPendentes
	}
	var out []MensagemOutbox
	for _, m := range o.msgs {
		if !o.enviada[m.ID] && !o.desistida[m.ID] && len(out) < limite {
			out = append(out, *m)
		}
	}
	return out, nil
}

func (o *outboxFake) MarcarEnviada(_ context.Context, id uuid.UUID) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.erroMarcar != nil {
		return o.erroMarcar
	}
	o.enviada[id] = true
	return nil
}

func (o *outboxFake) RegistrarFalha(_ context.Context, id uuid.UUID, desistir bool) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.erroFalha != nil {
		return o.erroFalha
	}
	for _, m := range o.msgs {
		if m.ID == id {
			m.Tentativas++
		}
	}
	if desistir {
		o.desistida[id] = true
	}
	return nil
}

// pubFake publica com respostas programadas por chamada.
type pubFake struct {
	mu        sync.Mutex
	publicado []Envelope
	filas     []string
	erros     []error // consumido em ordem; nil = sucesso
}

func (p *pubFake) Publicar(_ context.Context, fila string, env Envelope, _ map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.erros) > 0 {
		err := p.erros[0]
		p.erros = p.erros[1:]
		if err != nil {
			return err
		}
	}
	p.publicado = append(p.publicado, env)
	p.filas = append(p.filas, fila)
	return nil
}

func linha(t *testing.T, destino string) MensagemOutbox {
	return MensagemOutbox{ID: uuid.New(), Destino: destino, Envelope: envelopeValido(t, "Cmd")}
}

var destinos = map[string]string{"execution": filaExecution, "billing": "http://aws/billing-commands.fifo"}

func TestCiclo_PublicaEmOrdemEMarca(t *testing.T) {
	a, b := linha(t, "execution"), linha(t, "billing")
	o, p := novaOutbox(a, b), &pubFake{}

	n, err := NovoRelay(o, p, ConfigRelay{Destinos: destinos}).Ciclo(context.Background())

	if err != nil || n != 2 {
		t.Fatalf("Ciclo = %d, %v", n, err)
	}
	if p.publicado[0].MessageID != a.Envelope.MessageID || p.publicado[1].MessageID != b.Envelope.MessageID {
		t.Error("ordem de publicação diferente da ordem da outbox")
	}
	if p.filas[0] != filaExecution || p.filas[1] != destinos["billing"] {
		t.Errorf("destinos resolvidos errado: %v", p.filas)
	}
	if !o.enviada[a.ID] || !o.enviada[b.ID] || o.liberacoes != 1 {
		t.Error("mensagens não marcadas ou liderança não liberada")
	}
}

func TestCiclo_SemLiderancaNaoPublica(t *testing.T) {
	o, p := novaOutbox(linha(t, "execution")), &pubFake{}
	o.liderOcup = true
	n, err := NovoRelay(o, p, ConfigRelay{Destinos: destinos}).Ciclo(context.Background())
	if err != nil || n != 0 || len(p.publicado) != 0 {
		t.Fatalf("réplica não líder publicou: n=%d err=%v", n, err)
	}
}

func TestCiclo_ErroTransitorioParaSemPular(t *testing.T) {
	a, b := linha(t, "execution"), linha(t, "execution")
	o, p := novaOutbox(a, b), &pubFake{erros: []error{errInfra}}

	n, err := NovoRelay(o, p, ConfigRelay{Destinos: destinos}).Ciclo(context.Background())

	if !errors.Is(err, errInfra) || n != 0 {
		t.Fatalf("esperava parar no erro: n=%d err=%v", n, err)
	}
	if len(p.publicado) != 0 {
		t.Error("pular a mensagem com erro quebraria a ordem da saga")
	}
	if o.msgs[0].Tentativas != 1 || o.desistida[a.ID] {
		t.Error("tentativa deve ser registrada sem desistir")
	}
	if o.liberacoes != 1 {
		t.Error("liderança deve ser liberada mesmo com erro")
	}
}

func TestCiclo_DesisteAposMaxTentativasESegue(t *testing.T) {
	a, b := linha(t, "execution"), linha(t, "execution")
	a.Tentativas = 9
	o, p := novaOutbox(a, b), &pubFake{erros: []error{errInfra}}

	n, err := NovoRelay(o, p, ConfigRelay{Destinos: destinos}).Ciclo(context.Background())

	if err != nil || n != 1 {
		t.Fatalf("Ciclo = %d, %v", n, err)
	}
	if !o.desistida[a.ID] || !o.enviada[b.ID] {
		t.Error("linha venenosa deve ser desistida e não travar as outras sagas")
	}
}

func TestCiclo_ErroPermanenteDesisteNaPrimeira(t *testing.T) {
	a := linha(t, "execution")
	o, p := novaOutbox(a), &pubFake{erros: []error{Permanente(errors.New("envelope inválido"))}}
	if _, err := NovoRelay(o, p, ConfigRelay{Destinos: destinos}).Ciclo(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !o.desistida[a.ID] {
		t.Error("erro permanente não melhora com retentativa")
	}
}

func TestCiclo_DestinoDesconhecidoDesiste(t *testing.T) {
	a, b := linha(t, "inexistente"), linha(t, "execution")
	o, p := novaOutbox(a, b), &pubFake{}
	n, err := NovoRelay(o, p, ConfigRelay{Destinos: destinos}).Ciclo(context.Background())
	if err != nil || n != 1 || !o.desistida[a.ID] || !o.enviada[b.ID] {
		t.Fatalf("n=%d err=%v desistida=%v", n, err, o.desistida[a.ID])
	}
}

func TestCiclo_FalhasDaOutbox(t *testing.T) {
	casos := map[string]func(*outboxFake){
		"liderança":       func(o *outboxFake) { o.erroLider = errInfra },
		"pendentes":       func(o *outboxFake) { o.erroPendentes = errInfra },
		"marcar enviada":  func(o *outboxFake) { o.erroMarcar = errInfra },
		"registrar falha": func(o *outboxFake) { o.erroFalha = errInfra },
	}
	for nome, quebrar := range casos {
		t.Run(nome, func(t *testing.T) {
			o := novaOutbox(linha(t, "execution"))
			quebrar(o)
			p := &pubFake{}
			if nome == "registrar falha" {
				p.erros = []error{errors.New("sqs")}
			}
			if _, err := NovoRelay(o, p, ConfigRelay{Destinos: destinos}).Ciclo(context.Background()); !errors.Is(err, errInfra) {
				t.Errorf("esperava o erro da outbox, veio %v", err)
			}
		})
	}
}

func TestCiclo_FalhaDeRegistroAoDesistirDeDestinoDesconhecido(t *testing.T) {
	o := novaOutbox(linha(t, "inexistente"))
	o.erroFalha = errInfra
	if _, err := NovoRelay(o, &pubFake{}, ConfigRelay{Destinos: destinos}).Ciclo(context.Background()); !errors.Is(err, errInfra) {
		t.Errorf("esperava erro, veio %v", err)
	}
}

func TestCiclo_RespeitaLote(t *testing.T) {
	o, p := novaOutbox(linha(t, "execution"), linha(t, "execution"), linha(t, "execution")), &pubFake{}
	n, _ := NovoRelay(o, p, ConfigRelay{Destinos: destinos, Lote: 2}).Ciclo(context.Background())
	if n != 2 {
		t.Errorf("enviadas = %d, esperava o lote de 2", n)
	}
}

// Várias réplicas disputando a liderança: cada saga recebe suas mensagens em ordem.
func TestCiclo_ReplicasConcorrentesPreservamOrdemPorSaga(t *testing.T) {
	var msgs []MensagemOutbox
	for s := 0; s < 5; s++ {
		saga := uuid.New()
		for i := 0; i < 10; i++ {
			m := linha(t, "execution")
			m.Envelope.SagaID = saga
			m.Envelope.Versao = i + 1 // usado como número de sequência no teste
			msgs = append(msgs, m)
		}
	}
	o, p := novaOutbox(msgs...), &pubFake{}

	var wg sync.WaitGroup
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			relay := NovoRelay(o, p, ConfigRelay{Destinos: destinos, Lote: 7})
			for i := 0; i < 50; i++ {
				_, _ = relay.Ciclo(context.Background())
			}
		}()
	}
	wg.Wait()

	if len(p.publicado) != len(msgs) {
		t.Fatalf("publicadas %d de %d (duplicou ou perdeu)", len(p.publicado), len(msgs))
	}
	ultima := map[uuid.UUID]int{}
	for _, e := range p.publicado {
		if e.Versao <= ultima[e.SagaID] {
			t.Fatalf("saga %s fora de ordem: %d depois de %d", e.SagaID, e.Versao, ultima[e.SagaID])
		}
		ultima[e.SagaID] = e.Versao
	}
}

func TestExecutar_RodaCiclosAteCancelar(t *testing.T) {
	o, p := novaOutbox(linha(t, "execution")), &pubFake{erros: []error{errInfra}}
	ctx, cancelar := context.WithCancel(context.Background())
	feito := make(chan error, 1)
	go func() {
		feito <- NovoRelay(o, p, ConfigRelay{Destinos: destinos, Intervalo: time.Millisecond}).Executar(ctx)
	}()

	prazo := time.After(2 * time.Second)
	for {
		p.mu.Lock()
		n := len(p.publicado)
		p.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-prazo:
			t.Fatal("relay não publicou após o erro transitório")
		case <-time.After(time.Millisecond):
		}
	}
	cancelar()
	if err := <-feito; err != nil {
		t.Fatal(err)
	}
}

func TestNovoRelay_Padroes(t *testing.T) {
	r := NovoRelay(novaOutbox(), &pubFake{}, ConfigRelay{})
	if r.cfg.Intervalo != 500*time.Millisecond || r.cfg.Lote != 50 || r.cfg.MaxTentativas != 10 {
		t.Errorf("padrões = %+v", r.cfg)
	}
}
