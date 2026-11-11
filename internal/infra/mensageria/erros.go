package mensageria

import "errors"

// erroPermanente marca um erro que reentregar não resolve (payload inválido,
// tipo desconhecido, regra de negócio violada). O consumidor dá ack e registra
// o erro no log em vez de devolver a mensagem para a fila.
type erroPermanente struct{ causa error }

func (e erroPermanente) Error() string { return e.causa.Error() }
func (e erroPermanente) Unwrap() error { return e.causa }

// Permanente embrulha err como erro permanente. nil continua nil.
func Permanente(err error) error {
	if err == nil {
		return nil
	}
	return erroPermanente{causa: err}
}

// EhPermanente informa se err (ou algum erro da cadeia) é permanente.
//
// Classificação usada pelo Consumer (backlog F4-4.3):
//
//	nil                  → ack
//	permanente           → log error + ack (análise pelo log, não pela DLQ)
//	qualquer outro erro  → sem ack: o SQS reentrega após o visibility timeout
//	                       e manda para a DLQ após maxReceiveCount
//
// Um erro de negócio esperado (ex.: orçamento recusado) não é erro aqui: o
// handler responde a falha à saga pela outbox e devolve nil.
func EhPermanente(err error) bool {
	var p erroPermanente
	return errors.As(err, &p)
}
