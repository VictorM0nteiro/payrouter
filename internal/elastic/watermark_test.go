// watermark_test.go
// Testes da marca d'água contígua, sem goroutines.
// Objetivo: provar que a marca só avança sobre um prefixo sem buracos (a 7 terminando antes da
// 5 não move nada), que ela pula de uma vez quando o buraco fecha, que nunca recua, e que isso
// vale para qualquer ordem de conclusão.

package elastic

import (
	"math/rand"
	"testing"
)

func TestForaDeOrdemNaoAvancaAteOBuracoFechar(t *testing.T) {
	w := newWatermark()
	for range 10 {
		w.issue()
	}

	for _, seq := range []uint64{7, 6, 5} {
		if upTo, adv := w.complete(seq); adv {
			t.Fatalf("complete(%d) avançou a marca para %d com o prefixo 0..4 em aberto", seq, upTo)
		}
	}
	for seq := uint64(0); seq < 4; seq++ {
		if upTo, adv := w.complete(seq); !adv || upTo != seq {
			t.Fatalf("complete(%d) = %d,%v; esperado avançar para %d", seq, upTo, adv, seq)
		}
	}
	// A 4 fecha o buraco: a marca pula sobre 4, 5, 6 e 7 de uma vez.
	if upTo, adv := w.complete(4); !adv || upTo != 7 {
		t.Errorf("complete(4) = %d,%v; esperado pular para 7", upTo, adv)
	}
}

func TestSequenciaRepetidaOuJaCobertaEIgnorada(t *testing.T) {
	w := newWatermark()
	w.issue()
	w.issue()
	w.complete(0)

	if _, adv := w.complete(0); adv {
		t.Error("repetir uma sequência já coberta não pode avançar")
	}
	w.complete(1)
	if _, adv := w.complete(1); adv {
		t.Error("repetir a última também não")
	}
}

func TestQualquerOrdemDeConclusaoChegaAoFimSemNuncaPassarDeUmBuraco(t *testing.T) {
	const n = 300
	for seed := range int64(20) {
		w := newWatermark()
		for range n {
			w.issue()
		}
		finished := make([]bool, n)
		last := -1
		for _, seq := range rand.New(rand.NewSource(seed)).Perm(n) {
			finished[seq] = true
			upTo, adv := w.complete(uint64(seq))
			if !adv {
				continue
			}
			if int(upTo) <= last {
				t.Fatalf("seed %d: a marca recuou ou parou: %d depois de %d", seed, upTo, last)
			}
			for s := 0; s <= int(upTo); s++ {
				if !finished[s] {
					t.Fatalf("seed %d: marca %d à frente da tarefa %d, que não terminou", seed, upTo, s)
				}
			}
			last = int(upTo)
		}
		if last != n-1 {
			t.Fatalf("seed %d: marca final %d, esperado %d", seed, last, n-1)
		}
	}
}
