// Package dynamo implementa a persistência do Execution Service em DynamoDB,
// numa tabela única (single-table):
//
//	Tarefa    pk=TAREFA#<id>    sk=TAREFA       gsi1: FILA#<tipo>#<estado> / <prioridade>#<criado_em>#<id>
//	Ponteiro  pk=OS#<os_id>     sk=TAREFA#<id>  (lista as tarefas de uma OS)
//	Inbox     pk=MSG#<msg_id>   sk=MSG          ttl de 7 dias
//	Outbox    pk=OUTBOX#<id>    sk=OUTBOX       gsi1: OUTBOX#PENDENTE / <criado_em>#<seq>#<id> (esparso)
//
// Efeito, marca na inbox e mensagens de saída vão no mesmo TransactWriteItems.
package dynamo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/application/execucao"
	"github.com/ProblemaTheu/oficina-execution/internal/application/logging"
	"github.com/ProblemaTheu/oficina-execution/internal/domain/tarefa"
	"github.com/ProblemaTheu/oficina-execution/internal/infra/mensageria"
)

// DestinoOS é o destino lógico das respostas, resolvido pelo relay na fila
// os-saga-replies.fifo.
const DestinoOS = "os"

const (
	indiceGSI1     = "gsi1"
	outboxPendente = "OUTBOX#PENDENTE"
	ttlInbox       = 7 * 24 * time.Hour
)

// Cliente é o subconjunto do cliente DynamoDB usado — *dynamodb.Client satisfaz.
type Cliente interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// Repositorio implementa execucao.Repositorio e mensageria.OutboxStore.
type Repositorio struct {
	db     Cliente
	tabela string
	tracer mensageria.Tracer
	agora  func() time.Time
}

var (
	_ execucao.Repositorio   = (*Repositorio)(nil)
	_ mensageria.OutboxStore = (*Repositorio)(nil)
)

// Novo cria o repositório. tracer nil = sem propagação de trace.
func Novo(db Cliente, tabela string, tracer mensageria.Tracer) *Repositorio {
	if tracer == nil {
		tracer = mensageria.TracerNulo{}
	}
	return &Repositorio{db: db, tabela: tabela, tracer: tracer, agora: time.Now}
}

// ── itens ─────────────────────────────────────────────────────────────────────

type itemTarefa struct {
	PK           string `dynamodbav:"pk"`
	SK           string `dynamodbav:"sk"`
	GSI1PK       string `dynamodbav:"gsi1pk"`
	GSI1SK       string `dynamodbav:"gsi1sk"`
	ID           string `dynamodbav:"id"`
	OsID         string `dynamodbav:"os_id"`
	SagaID       string `dynamodbav:"saga_id"`
	NumeroOS     string `dynamodbav:"numero_os,omitempty"`
	Tipo         string `dynamodbav:"tipo"`
	Estado       string `dynamodbav:"estado"`
	Placa        string `dynamodbav:"placa,omitempty"`
	Modelo       string `dynamodbav:"modelo,omitempty"`
	Descricao    string `dynamodbav:"descricao,omitempty"`
	Mecanico     string `dynamodbav:"mecanico,omitempty"`
	Diagnostico  string `dynamodbav:"diagnostico,omitempty"`
	CriadoEm     string `dynamodbav:"criado_em"`
	IniciadoEm   string `dynamodbav:"iniciado_em,omitempty"`
	FinalizadoEm string `dynamodbav:"finalizado_em,omitempty"`
}

func chaveTarefa(id uuid.UUID) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "TAREFA#" + id.String()},
		"sk": &types.AttributeValueMemberS{Value: "TAREFA"},
	}
}

func formatar(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func paraItem(t tarefa.Tarefa) itemTarefa {
	criado := t.CriadoEm
	return itemTarefa{
		PK: "TAREFA#" + t.ID.String(), SK: "TAREFA",
		GSI1PK: fmt.Sprintf("FILA#%s#%s", t.Tipo, t.Estado),
		GSI1SK: fmt.Sprintf("%d#%s#%s", t.Tipo.Prioridade(), formatar(&criado), t.ID),
		ID:     t.ID.String(), OsID: t.OsID.String(), SagaID: t.SagaID.String(), NumeroOS: t.NumeroOS,
		Tipo: string(t.Tipo), Estado: string(t.Estado), Placa: t.Placa, Modelo: t.Modelo, Descricao: t.Descricao,
		Mecanico: t.Mecanico, Diagnostico: t.Diagnostico,
		CriadoEm: formatar(&criado), IniciadoEm: formatar(t.IniciadoEm), FinalizadoEm: formatar(t.FinalizadoEm),
	}
}

func instante(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	return &t, err
}

func deItem(i itemTarefa) (tarefa.Tarefa, error) {
	var (
		t   tarefa.Tarefa
		err error
	)
	if t.ID, err = uuid.Parse(i.ID); err != nil {
		return t, fmt.Errorf("id da tarefa: %w", err)
	}
	if t.OsID, err = uuid.Parse(i.OsID); err != nil {
		return t, fmt.Errorf("os_id: %w", err)
	}
	if t.SagaID, err = uuid.Parse(i.SagaID); err != nil {
		return t, fmt.Errorf("saga_id: %w", err)
	}
	criado, err := instante(i.CriadoEm)
	if err != nil || criado == nil {
		return t, fmt.Errorf("criado_em: %v", err)
	}
	if t.IniciadoEm, err = instante(i.IniciadoEm); err != nil {
		return t, err
	}
	if t.FinalizadoEm, err = instante(i.FinalizadoEm); err != nil {
		return t, err
	}
	t.CriadoEm, t.NumeroOS, t.Tipo, t.Estado = *criado, i.NumeroOS, tarefa.Tipo(i.Tipo), tarefa.Estado(i.Estado)
	t.Placa, t.Modelo, t.Descricao, t.Mecanico, t.Diagnostico = i.Placa, i.Modelo, i.Descricao, i.Mecanico, i.Diagnostico
	return t, nil
}

// ── escrita atômica ───────────────────────────────────────────────────────────

func (r *Repositorio) put(item any, condicao string, valores map[string]types.AttributeValue) (types.TransactWriteItem, error) {
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return types.TransactWriteItem{}, err
	}
	p := &types.Put{TableName: aws.String(r.tabela), Item: av}
	if condicao != "" {
		p.ConditionExpression = aws.String(condicao)
		p.ExpressionAttributeValues = valores
	}
	return types.TransactWriteItem{Put: p}, nil
}

func (r *Repositorio) itemInbox(cmd execucao.Comando) (types.TransactWriteItem, error) {
	agora := r.agora().UTC()
	return r.put(map[string]any{
		"pk": "MSG#" + cmd.MessageID.String(), "sk": "MSG", "tipo": cmd.Tipo,
		"processado_em": agora.Format(time.RFC3339Nano), "ttl": agora.Add(ttlInbox).Unix(),
	}, "attribute_not_exists(pk)", nil)
}

// itensOutbox converte as saídas em envelopes e itens de outbox. O seq mantém
// a ordem entre mensagens gravadas na mesma transação (mesmo instante).
func (r *Repositorio) itensOutbox(ctx context.Context, saidas []execucao.Saida) ([]types.TransactWriteItem, error) {
	trace := r.tracer.Capturar(ctx)
	agora := r.agora().UTC()
	var out []types.TransactWriteItem
	for i, s := range saidas {
		env, err := mensageria.Novo(s.Tipo, s.SagaID, s.OsID, logging.Correlacao(ctx), mensageria.OrigemExecution, s.Payload, agora)
		if err != nil {
			return nil, err
		}
		corpo, err := json.Marshal(env)
		if err != nil {
			return nil, err
		}
		id := uuid.New()
		item, err := r.put(map[string]any{
			"pk": "OUTBOX#" + id.String(), "sk": "OUTBOX", "id": id.String(), "destino": DestinoOS,
			"envelope": string(corpo), "trace": trace, "tentativas": 0, "criado_em": agora.Format(time.RFC3339Nano),
			"gsi1pk": outboxPendente, "gsi1sk": fmt.Sprintf("%s#%03d#%s", agora.Format(time.RFC3339Nano), i, id),
		}, "", nil)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

// transacao executa os itens e traduz a condição falha do primeiro item no
// erro de negócio informado (inbox → ErrJaProcessada, tarefa → ErrConflito).
func (r *Repositorio) transacao(ctx context.Context, itens []types.TransactWriteItem, seCondicaoFalhar error) error {
	_, err := r.db.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: itens})
	var cancelada *types.TransactionCanceledException
	if errors.As(err, &cancelada) && len(cancelada.CancellationReasons) > 0 &&
		aws.ToString(cancelada.CancellationReasons[0].Code) == "ConditionalCheckFailed" {
		return seCondicaoFalhar
	}
	if err != nil {
		return fmt.Errorf("dynamodb TransactWriteItems: %w", err)
	}
	return nil
}

// CriarDeComando grava inbox + tarefa + ponteiro da OS + saídas.
func (r *Repositorio) CriarDeComando(ctx context.Context, t tarefa.Tarefa, cmd execucao.Comando, saidas []execucao.Saida) error {
	inbox, err := r.itemInbox(cmd)
	if err != nil {
		return err
	}
	tf, err := r.put(paraItem(t), "attribute_not_exists(pk)", nil)
	if err != nil {
		return err
	}
	ponteiro, err := r.put(map[string]any{
		"pk": "OS#" + t.OsID.String(), "sk": "TAREFA#" + t.ID.String(), "tarefa_id": t.ID.String(),
	}, "", nil)
	if err != nil {
		return err
	}
	out, err := r.itensOutbox(ctx, saidas)
	if err != nil {
		return err
	}
	return r.transacao(ctx, append([]types.TransactWriteItem{inbox, tf, ponteiro}, out...), execucao.ErrJaProcessada)
}

// Responder grava inbox + saídas, sem tarefa.
func (r *Repositorio) Responder(ctx context.Context, cmd execucao.Comando, saidas []execucao.Saida) error {
	inbox, err := r.itemInbox(cmd)
	if err != nil {
		return err
	}
	out, err := r.itensOutbox(ctx, saidas)
	if err != nil {
		return err
	}
	return r.transacao(ctx, append([]types.TransactWriteItem{inbox}, out...), execucao.ErrJaProcessada)
}

// Salvar regrava a tarefa condicionada ao estado anterior (sem corrida entre
// dois mecânicos) e grava as saídas.
func (r *Repositorio) Salvar(ctx context.Context, t tarefa.Tarefa, anterior tarefa.Estado, saidas []execucao.Saida) error {
	tf, err := r.put(paraItem(t), "estado = :anterior", map[string]types.AttributeValue{
		":anterior": &types.AttributeValueMemberS{Value: string(anterior)},
	})
	if err != nil {
		return err
	}
	out, err := r.itensOutbox(ctx, saidas)
	if err != nil {
		return err
	}
	return r.transacao(ctx, append([]types.TransactWriteItem{tf}, out...), execucao.ErrConflito)
}

// ── leitura ───────────────────────────────────────────────────────────────────

// Buscar devolve uma tarefa pelo id (leitura fortemente consistente).
func (r *Repositorio) Buscar(ctx context.Context, id uuid.UUID) (tarefa.Tarefa, error) {
	out, err := r.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tabela), Key: chaveTarefa(id), ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return tarefa.Tarefa{}, fmt.Errorf("dynamodb GetItem: %w", err)
	}
	if len(out.Item) == 0 {
		return tarefa.Tarefa{}, execucao.ErrNaoEncontrada
	}
	var i itemTarefa
	if err := attributevalue.UnmarshalMap(out.Item, &i); err != nil {
		return tarefa.Tarefa{}, err
	}
	return deItem(i)
}

func (r *Repositorio) consultar(ctx context.Context, in *dynamodb.QueryInput) ([]map[string]types.AttributeValue, error) {
	var itens []map[string]types.AttributeValue
	for {
		out, err := r.db.Query(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("dynamodb Query: %w", err)
		}
		itens = append(itens, out.Items...)
		if len(out.LastEvaluatedKey) == 0 || (in.Limit != nil && len(itens) >= int(*in.Limit)) {
			return itens, nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// ListarFila devolve as tarefas AGUARDANDO de um tipo, por ordem de chegada.
func (r *Repositorio) ListarFila(ctx context.Context, tipo tarefa.Tipo) ([]tarefa.Tarefa, error) {
	itens, err := r.consultar(ctx, &dynamodb.QueryInput{
		TableName: aws.String(r.tabela), IndexName: aws.String(indiceGSI1),
		KeyConditionExpression: aws.String("gsi1pk = :p"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: fmt.Sprintf("FILA#%s#%s", tipo, tarefa.Aguardando)},
		},
		ScanIndexForward: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	out := make([]tarefa.Tarefa, 0, len(itens))
	for _, av := range itens {
		var i itemTarefa
		if err := attributevalue.UnmarshalMap(av, &i); err != nil {
			return nil, err
		}
		t, err := deItem(i)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// ListarPorOS segue os ponteiros da OS e lê cada tarefa.
func (r *Repositorio) ListarPorOS(ctx context.Context, osID uuid.UUID) ([]tarefa.Tarefa, error) {
	itens, err := r.consultar(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tabela),
		KeyConditionExpression: aws.String("pk = :p AND begins_with(sk, :s)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: "OS#" + osID.String()},
			":s": &types.AttributeValueMemberS{Value: "TAREFA#"},
		},
	})
	if err != nil {
		return nil, err
	}
	var out []tarefa.Tarefa
	for _, av := range itens {
		var p struct {
			TarefaID string `dynamodbav:"tarefa_id"`
		}
		if err := attributevalue.UnmarshalMap(av, &p); err != nil {
			return nil, err
		}
		id, err := uuid.Parse(p.TarefaID)
		if err != nil {
			return nil, err
		}
		t, err := r.Buscar(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// limite32 converte o tamanho do lote para o Limit do DynamoDB, com teto.
func limite32(n int) int32 {
	switch {
	case n < 1:
		return 1
	case n > math.MaxInt32:
		return math.MaxInt32
	default:
		return int32(n) // #nosec G115 -- limitado acima
	}
}
