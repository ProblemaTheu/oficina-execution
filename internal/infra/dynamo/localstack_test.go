//go:build localstack

// Testes do adaptador contra DynamoDB de verdade (LocalStack):
//
//	docker run -d -p 4566:4566 -e SERVICES=dynamodb,sqs localstack/localstack:4
//	AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
//	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//	go test -tags localstack ./internal/infra/dynamo/
package dynamo

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/application/execucao"
	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
	"github.com/ProblemaTheu/oficina-execution/internal/domain/tarefa"
	"github.com/ProblemaTheu/oficina-execution/internal/infra/mensageria"
)

// tabelaDeTeste cria uma tabela com o mesmo schema do localstack-init.sh do
// oficina-app (pk/sk + gsi1) e a remove no fim.
func tabelaDeTeste(t *testing.T) *Repositorio {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL") == "" {
		t.Skip("AWS_ENDPOINT_URL não definido — suba o LocalStack")
	}
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	db := dynamodb.NewFromConfig(cfg)
	nome := "teste-" + uuid.NewString()[:8]
	s := func(n string) types.AttributeDefinition {
		return types.AttributeDefinition{AttributeName: aws.String(n), AttributeType: types.ScalarAttributeTypeS}
	}
	chave := func(n string, k types.KeyType) types.KeySchemaElement {
		return types.KeySchemaElement{AttributeName: aws.String(n), KeyType: k}
	}
	_, err = db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(nome),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{s("pk"), s("sk"), s("gsi1pk"), s("gsi1sk")},
		KeySchema:            []types.KeySchemaElement{chave("pk", types.KeyTypeHash), chave("sk", types.KeyTypeRange)},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName:  aws.String("gsi1"),
			KeySchema:  []types.KeySchemaElement{chave("gsi1pk", types.KeyTypeHash), chave("gsi1sk", types.KeyTypeRange)},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
	})
	if err != nil {
		t.Fatalf("criar tabela: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(nome)})
	})
	return Novo(db, nome, nil)
}

func novaTarefa(t *testing.T, tipo tarefa.Tipo, osID uuid.UUID, criado time.Time) tarefa.Tarefa {
	t.Helper()
	tf, err := tarefa.Nova(tipo, osID, uuid.New(), "OS-2026-00001", "ABC1D23", "Gol", "barulho", criado)
	if err != nil {
		t.Fatal(err)
	}
	return tf
}

func cmdPara(tf tarefa.Tarefa) execucao.Comando {
	return execucao.Comando{MessageID: uuid.New(), Tipo: execucao.TipoIniciarDiagnostico, SagaID: tf.SagaID, OsID: tf.OsID}
}

func TestLocalStack_CriarDeComandoEIdempotencia(t *testing.T) {
	r := tabelaDeTeste(t)
	ctx := context.Background()
	tf := novaTarefa(t, tarefa.Diagnostico, uuid.New(), time.Now())
	cmd := cmdPara(tf)

	if err := r.CriarDeComando(ctx, tf, cmd, nil); err != nil {
		t.Fatalf("CriarDeComando: %v", err)
	}
	lida, err := r.Buscar(ctx, tf.ID)
	if err != nil || lida.OsID != tf.OsID || lida.Estado != tarefa.Aguardando || lida.Placa != "ABC1D23" || !lida.CriadoEm.Equal(tf.CriadoEm) {
		t.Fatalf("round-trip: %+v (%v)", lida, err)
	}

	// Reentrega do mesmo comando (mesmo message_id), com outra tarefa gerada.
	outra := novaTarefa(t, tarefa.Diagnostico, tf.OsID, time.Now())
	if err := r.CriarDeComando(ctx, outra, cmd, nil); !errors.Is(err, execucao.ErrJaProcessada) {
		t.Fatalf("esperava ErrJaProcessada, veio %v", err)
	}
	if ts, _ := r.ListarPorOS(ctx, tf.OsID); len(ts) != 1 {
		t.Errorf("reentrega duplicou a tarefa: %d", len(ts))
	}
	if _, err := r.Buscar(ctx, uuid.New()); !errors.Is(err, execucao.ErrNaoEncontrada) {
		t.Errorf("tarefa inexistente: %v", err)
	}
}

func TestLocalStack_SalvarCondicionadoAoEstado(t *testing.T) {
	r := tabelaDeTeste(t)
	ctx := context.Background()
	tf := novaTarefa(t, tarefa.Diagnostico, uuid.New(), time.Now())
	_ = r.CriarDeComando(ctx, tf, cmdPara(tf), nil)

	mec1 := tf
	if err := mec1.Iniciar("mec-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := r.Salvar(ctx, mec1, tarefa.Aguardando, nil); err != nil {
		t.Fatalf("Salvar: %v", err)
	}
	// Segundo mecânico leu a tarefa ainda AGUARDANDO e tenta salvar por cima.
	mec2 := tf
	_ = mec2.Iniciar("mec-2", time.Now())
	if err := r.Salvar(ctx, mec2, tarefa.Aguardando, nil); !errors.Is(err, execucao.ErrConflito) {
		t.Fatalf("esperava ErrConflito, veio %v", err)
	}
	if lida, _ := r.Buscar(ctx, tf.ID); lida.Mecanico != "mec-1" || lida.IniciadoEm == nil {
		t.Errorf("o segundo mecânico sobrescreveu: %+v", lida)
	}
}

func TestLocalStack_FilaSoAguardandoEmOrdem(t *testing.T) {
	r := tabelaDeTeste(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	antiga := novaTarefa(t, tarefa.Diagnostico, uuid.New(), base)
	nova := novaTarefa(t, tarefa.Diagnostico, uuid.New(), base.Add(10*time.Minute))
	iniciada := novaTarefa(t, tarefa.Diagnostico, uuid.New(), base.Add(-time.Minute))
	for _, tf := range []tarefa.Tarefa{nova, antiga, iniciada} {
		if err := r.CriarDeComando(ctx, tf, cmdPara(tf), nil); err != nil {
			t.Fatal(err)
		}
	}
	emAndamento := iniciada
	_ = emAndamento.Iniciar("mec", time.Now())
	if err := r.Salvar(ctx, emAndamento, tarefa.Aguardando, nil); err != nil {
		t.Fatal(err)
	}

	fila, err := r.ListarFila(ctx, tarefa.Diagnostico)
	if err != nil {
		t.Fatal(err)
	}
	if len(fila) != 2 || fila[0].ID != antiga.ID || fila[1].ID != nova.ID {
		t.Fatalf("fila = %v; esperava só as aguardando, da mais antiga para a mais nova", fila)
	}
	if reparos, _ := r.ListarFila(ctx, tarefa.Reparo); len(reparos) != 0 {
		t.Errorf("fila de reparo deveria estar vazia: %d", len(reparos))
	}
}

func TestLocalStack_SaidasNaOutboxERelay(t *testing.T) {
	r := tabelaDeTeste(t)
	ctx := logging.ComCorrelacao(context.Background(), "req-exec")
	tf := novaTarefa(t, tarefa.Diagnostico, uuid.New(), time.Now())
	_ = r.CriarDeComando(ctx, tf, cmdPara(tf), nil)

	concluida := tf
	_ = concluida.ConcluirDiagnostico("troca do filtro", "mec", time.Now())
	saidas := []execucao.Saida{
		{Tipo: execucao.TipoDiagnosticoIniciado, SagaID: tf.SagaID, OsID: tf.OsID, Payload: map[string]string{"tarefa_id": tf.ID.String()}},
		{Tipo: execucao.TipoDiagnosticoConcluido, SagaID: tf.SagaID, OsID: tf.OsID, Payload: map[string]string{"diagnostico": "troca do filtro"}},
	}
	if err := r.Salvar(ctx, concluida, tarefa.Aguardando, saidas); err != nil {
		t.Fatal(err)
	}

	pend, err := r.Pendentes(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pend) != 2 || pend[0].Envelope.Tipo != execucao.TipoDiagnosticoIniciado || pend[1].Envelope.Tipo != execucao.TipoDiagnosticoConcluido {
		t.Fatalf("outbox = %+v; esperava as 2 saídas na ordem gravada", pend)
	}
	env := pend[1].Envelope
	if env.SagaID != tf.SagaID || env.OsID != tf.OsID || env.Origem != mensageria.OrigemExecution || env.CorrelationID != "req-exec" || pend[1].Destino != DestinoOS {
		t.Errorf("envelope errado: %+v", env)
	}

	// Relay real do pacote de mensageria sobre a outbox DynamoDB.
	pub := &publicadorMemoria{}
	relay := mensageria.NovoRelay(r, pub, mensageria.ConfigRelay{Destinos: map[string]string{DestinoOS: "fila-respostas"}})
	if n, err := relay.Ciclo(ctx); err != nil || n != 2 {
		t.Fatalf("Ciclo = %d, %v", n, err)
	}
	if pub.tipos[0] != execucao.TipoDiagnosticoIniciado || pub.tipos[1] != execucao.TipoDiagnosticoConcluido {
		t.Errorf("relay fora de ordem: %v", pub.tipos)
	}
	time.Sleep(500 * time.Millisecond) // o GSI é eventualmente consistente
	if resto, _ := r.Pendentes(ctx, 10); len(resto) != 0 {
		t.Errorf("enviadas continuam pendentes: %d", len(resto))
	}
}

func TestLocalStack_RespondereRegistrarFalha(t *testing.T) {
	r := tabelaDeTeste(t)
	ctx := context.Background()
	cmd := execucao.Comando{MessageID: uuid.New(), Tipo: execucao.TipoIniciarDiagnostico, SagaID: uuid.New(), OsID: uuid.New()}
	falhou := []execucao.Saida{{Tipo: execucao.TipoDiagnosticoFalhou, SagaID: cmd.SagaID, OsID: cmd.OsID, Payload: map[string]string{"motivo": "x"}}}

	if err := r.Responder(ctx, cmd, falhou); err != nil {
		t.Fatal(err)
	}
	if err := r.Responder(ctx, cmd, falhou); !errors.Is(err, execucao.ErrJaProcessada) {
		t.Fatalf("resposta repetida: %v", err)
	}
	pend, _ := r.Pendentes(ctx, 10)
	if len(pend) != 1 {
		t.Fatalf("esperava 1 resposta pendente, há %d", len(pend))
	}
	if err := r.RegistrarFalha(ctx, pend[0].ID, false); err != nil {
		t.Fatal(err)
	}
	if again, _ := r.Pendentes(ctx, 10); len(again) != 1 || again[0].Tentativas != 1 {
		t.Fatalf("tentativa não contada: %+v", again)
	}
	if err := r.RegistrarFalha(ctx, pend[0].ID, true); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if resto, _ := r.Pendentes(ctx, 10); len(resto) != 0 {
		t.Errorf("mensagem desistida continua na fila do relay: %d", len(resto))
	}
}

type publicadorMemoria struct{ tipos []string }

func (p *publicadorMemoria) Publicar(_ context.Context, _ string, env mensageria.Envelope, _ map[string]string) error {
	p.tipos = append(p.tipos, env.Tipo)
	return nil
}

func TestLocalStack_SalvarVariasDeComando(t *testing.T) {
	r := tabelaDeTeste(t)
	ctx := context.Background()
	osID := uuid.New()
	a, b := novaTarefa(t, tarefa.Diagnostico, osID, time.Now()), novaTarefa(t, tarefa.Reparo, osID, time.Now())
	for _, tf := range []tarefa.Tarefa{a, b} {
		if err := r.CriarDeComando(ctx, tf, cmdPara(tf), nil); err != nil {
			t.Fatal(err)
		}
	}
	ca, cb := a, b
	ca.Cancelar(time.Now())
	cb.Cancelar(time.Now())
	cmd := execucao.Comando{MessageID: uuid.New(), Tipo: execucao.TipoCancelarTarefas, SagaID: a.SagaID, OsID: osID}
	resp := []execucao.Saida{{Tipo: execucao.TipoTarefasCanceladas, SagaID: a.SagaID, OsID: osID, Payload: map[string]int{"quantidade": 2}}}

	if err := r.SalvarVariasDeComando(ctx, []tarefa.Tarefa{ca, cb}, []tarefa.Estado{tarefa.Aguardando, tarefa.Aguardando}, cmd, resp); err != nil {
		t.Fatal(err)
	}
	ts, _ := r.ListarPorOS(ctx, osID)
	for _, tf := range ts {
		if tf.Estado != tarefa.Cancelada {
			t.Errorf("tarefa %s ficou %s", tf.Tipo, tf.Estado)
		}
	}
	if err := r.SalvarVariasDeComando(ctx, nil, nil, cmd, resp); !errors.Is(err, execucao.ErrJaProcessada) {
		t.Errorf("reentrega: %v", err)
	}
	// Estado anterior desatualizado: nada é gravado (tudo ou nada).
	outro := execucao.Comando{MessageID: uuid.New(), Tipo: execucao.TipoCancelarTarefas, SagaID: a.SagaID, OsID: osID}
	if err := r.SalvarVariasDeComando(ctx, []tarefa.Tarefa{ca}, []tarefa.Estado{tarefa.Aguardando}, outro, resp); !errors.Is(err, execucao.ErrConflito) {
		t.Errorf("estado mudou no meio: %v", err)
	}
	if err := r.SalvarVariasDeComando(ctx, []tarefa.Tarefa{ca}, nil, outro, resp); err == nil {
		t.Error("tamanhos diferentes devem falhar")
	}
}
