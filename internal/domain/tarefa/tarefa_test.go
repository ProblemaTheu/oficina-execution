package tarefa

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var agora = time.Date(2026, 11, 11, 15, 0, 0, 0, time.UTC)

func nova(t *testing.T, tipo Tipo) Tarefa {
	t.Helper()
	tf, err := Nova(tipo, uuid.New(), uuid.New(), "OS-2026-00001", " abc1d23 ", "Gol", "barulho", agora)
	if err != nil {
		t.Fatal(err)
	}
	return tf
}

func TestNova(t *testing.T) {
	tf := nova(t, Diagnostico)
	if tf.ID == uuid.Nil || tf.Estado != Aguardando || tf.Placa != "ABC1D23" {
		t.Errorf("tarefa nova errada: %+v", tf)
	}
	if _, err := Nova("PINTURA", uuid.New(), uuid.New(), "", "", "", "", agora); err == nil {
		t.Error("tipo inválido deve falhar")
	}
	if _, err := Nova(Reparo, uuid.Nil, uuid.New(), "", "", "", "", agora); err == nil {
		t.Error("os_id vazio deve falhar")
	}
}

func TestPrioridade_ReparoAntesDeDiagnostico(t *testing.T) {
	if Reparo.Prioridade() >= Diagnostico.Prioridade() {
		t.Error("reparo (carro já pago) vem antes de diagnóstico na fila")
	}
}

func TestIniciar(t *testing.T) {
	tf := nova(t, Diagnostico)
	if err := tf.Iniciar("", agora); !errors.Is(err, ErrMecanicoObrigatorio) {
		t.Errorf("sem mecânico: %v", err)
	}
	if err := tf.Iniciar("mec-1", agora); err != nil || tf.Estado != EmAndamento || tf.Mecanico != "mec-1" || tf.IniciadoEm == nil {
		t.Fatalf("Iniciar: %v %+v", err, tf)
	}
	if err := tf.Iniciar("mec-2", agora); !errors.Is(err, ErrTransicaoInvalida) {
		t.Errorf("iniciar duas vezes: %v", err)
	}
	tf.Estado = Cancelada
	if err := tf.Iniciar("mec-1", agora); !errors.Is(err, ErrTarefaCancelada) {
		t.Errorf("iniciar cancelada: %v", err)
	}
}

func TestConcluirDiagnostico(t *testing.T) {
	casos := []struct {
		nome   string
		montar func(*Tarefa)
		diag   string
		erro   error
	}{
		{"em andamento conclui", func(tf *Tarefa) { _ = tf.Iniciar("mec", agora) }, "troca do filtro", nil},
		{"aguardando conclui direto", func(*Tarefa) {}, "troca do filtro", nil},
		{"diagnóstico vazio", func(*Tarefa) {}, "  ", ErrDiagnosticoVazio},
		{"cancelada", func(tf *Tarefa) { tf.Estado = Cancelada }, "x", ErrTarefaCancelada},
		{"já concluída", func(tf *Tarefa) { tf.Estado = Concluida }, "x", ErrTransicaoInvalida},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			tf := nova(t, Diagnostico)
			c.montar(&tf)
			err := tf.ConcluirDiagnostico(c.diag, "mec", agora)
			if !errors.Is(err, c.erro) {
				t.Fatalf("erro = %v, esperava %v", err, c.erro)
			}
			if c.erro == nil && (tf.Estado != Concluida || tf.Diagnostico == "" || tf.FinalizadoEm == nil || tf.IniciadoEm == nil) {
				t.Errorf("conclusão incompleta: %+v", tf)
			}
		})
	}
}

func TestConcluirDiagnostico_SoEmTarefaDeDiagnostico(t *testing.T) {
	tf := nova(t, Reparo)
	if err := tf.ConcluirDiagnostico("x", "mec", agora); !errors.Is(err, ErrTipoErrado) {
		t.Errorf("reparo não tem diagnóstico: %v", err)
	}
}

func TestConcluirDiagnostico_AguardandoSemMecanico(t *testing.T) {
	tf := nova(t, Diagnostico)
	if err := tf.ConcluirDiagnostico("laudo", "", agora); !errors.Is(err, ErrMecanicoObrigatorio) {
		t.Errorf("concluir direto exige mecânico: %v", err)
	}
}

func TestConcluirReparo(t *testing.T) {
	r := nova(t, Reparo)
	if err := r.ConcluirReparo(agora); !errors.Is(err, ErrTransicaoInvalida) {
		t.Errorf("reparo precisa ser iniciado antes (pivô): %v", err)
	}
	_ = r.Iniciar("mec", agora)
	if err := r.ConcluirReparo(agora); err != nil || r.Estado != Concluida || r.FinalizadoEm == nil {
		t.Fatalf("concluir: %v %+v", err, r)
	}
	d := nova(t, Diagnostico)
	if err := d.ConcluirReparo(agora); !errors.Is(err, ErrTipoErrado) {
		t.Errorf("diagnóstico não conclui como reparo: %v", err)
	}
	c := nova(t, Reparo)
	c.Estado = Cancelada
	if err := c.ConcluirReparo(agora); !errors.Is(err, ErrTarefaCancelada) {
		t.Errorf("cancelada: %v", err)
	}
}

func TestCancelar(t *testing.T) {
	casos := map[Estado]bool{Aguardando: true, EmAndamento: true, Concluida: false, Cancelada: false}
	for estado, muda := range casos {
		tf := nova(t, Reparo)
		tf.Estado = estado
		if got := tf.Cancelar(agora); got != muda {
			t.Errorf("%s.Cancelar() = %v, esperava %v", estado, got, muda)
		}
		if muda && (tf.Estado != Cancelada || tf.FinalizadoEm == nil) {
			t.Errorf("%s: não cancelou: %+v", estado, tf)
		}
		if !muda && tf.Estado != estado {
			t.Errorf("%s: estado alterado indevidamente", estado)
		}
	}
}

func TestPlacaDeFalhaSimulada(t *testing.T) {
	for placa, falha := range map[string]bool{"FAL1A23": true, " fal9z99 ": true, "ABC1D23": false, "": false, "AFAL123": false} {
		if PlacaDeFalhaSimulada(placa) != falha {
			t.Errorf("%q: esperava %v", placa, falha)
		}
	}
}
