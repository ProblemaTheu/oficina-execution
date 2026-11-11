package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/ProblemaTheu/oficina-execution/internal/infra/mensageria"
)

// TentarLideranca sempre concede a liderança: o Execution roda com UMA réplica
// (corte 7′ do plano da Fase 4), então só existe um relay. Com mais réplicas,
// mensagens da mesma saga poderiam sair fora de ordem — a evolução é um lease
// em RELAY#LIDER com UpdateItem condicional (arquitetura §6.3).
func (r *Repositorio) TentarLideranca(context.Context) (func(), bool, error) {
	return func() {}, true, nil
}

// Pendentes lê o índice esparso da outbox em ordem de gravação.
//
// O GSI é eventualmente consistente: uma mensagem recém-marcada como enviada
// pode reaparecer no ciclo seguinte e ser republicada — o
// MessageDeduplicationId do FIFO e a inbox do OS absorvem a duplicata.
func (r *Repositorio) Pendentes(ctx context.Context, limite int) ([]mensageria.MensagemOutbox, error) {
	itens, err := r.consultar(ctx, &dynamodb.QueryInput{
		TableName: aws.String(r.tabela), IndexName: aws.String(indiceGSI1),
		KeyConditionExpression: aws.String("gsi1pk = :p"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: outboxPendente},
		},
		ScanIndexForward: aws.Bool(true),
		Limit:            aws.Int32(limite32(limite)),
	})
	if err != nil {
		return nil, err
	}
	out := make([]mensageria.MensagemOutbox, 0, len(itens))
	for _, av := range itens {
		var i struct {
			ID         string            `dynamodbav:"id"`
			Destino    string            `dynamodbav:"destino"`
			Envelope   string            `dynamodbav:"envelope"`
			Trace      map[string]string `dynamodbav:"trace"`
			Tentativas int               `dynamodbav:"tentativas"`
		}
		if err := attributevalue.UnmarshalMap(av, &i); err != nil {
			return nil, fmt.Errorf("outbox: %w", err)
		}
		id, err := uuid.Parse(i.ID)
		if err != nil {
			return nil, fmt.Errorf("outbox id: %w", err)
		}
		var env mensageria.Envelope
		if err := json.Unmarshal([]byte(i.Envelope), &env); err != nil {
			return nil, fmt.Errorf("outbox envelope %s: %w", id, err)
		}
		out = append(out, mensageria.MensagemOutbox{ID: id, Destino: i.Destino, Envelope: env, Trace: i.Trace, Tentativas: i.Tentativas})
	}
	return out, nil
}

func chaveOutbox(id uuid.UUID) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "OUTBOX#" + id.String()},
		"sk": &types.AttributeValueMemberS{Value: "OUTBOX"},
	}
}

// MarcarEnviada tira a mensagem do índice esparso.
func (r *Repositorio) MarcarEnviada(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tabela), Key: chaveOutbox(id),
		UpdateExpression: aws.String("SET enviado_em = :t REMOVE gsi1pk, gsi1sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": &types.AttributeValueMemberS{Value: r.agora().UTC().Format(time.RFC3339Nano)},
		},
	})
	return err
}

// RegistrarFalha conta a tentativa; com desistir, tira a mensagem do índice.
func (r *Repositorio) RegistrarFalha(ctx context.Context, id uuid.UUID, desistir bool) error {
	expr := "ADD tentativas :um"
	valores := map[string]types.AttributeValue{":um": &types.AttributeValueMemberN{Value: "1"}}
	if desistir {
		expr = "SET falhou_em = :t ADD tentativas :um REMOVE gsi1pk, gsi1sk"
		valores[":t"] = &types.AttributeValueMemberS{Value: r.agora().UTC().Format(time.RFC3339Nano)}
	}
	_, err := r.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tabela), Key: chaveOutbox(id),
		UpdateExpression: aws.String(expr), ExpressionAttributeValues: valores,
	})
	return err
}
