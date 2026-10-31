// policy_test.go
// Testes da política de redimensionamento, determinísticos: sem goroutines, sem relógio.
// Objetivo: provar a histerese. Cresce só depois de amostras seguidas com pressão, encolhe só
// depois de muito mais amostras ociosas, uma amostra calma zera a contagem, respeita o piso e o
// teto, faz pausa depois de mudar, e uma carga que alterna entre cheia e vazia não faz o pool
// oscilar.

package elastic

import (
	"testing"
	"time"
)

var (
	loaded = Sample{Size: 2, Min: 1, Max: 4, Ready: 5, Wait: 50 * time.Millisecond, Busy: 1}
	idle   = Sample{Size: 3, Min: 1, Max: 4, Ready: 0, Wait: 0, Busy: 0.1}
	calm   = Sample{Size: 2, Min: 1, Max: 4, Ready: 0, Wait: 0, Busy: 0.6} // nem pressão nem ociosidade
)

func testPolicy() Policy {
	return Policy{GrowWait: 10 * time.Millisecond, GrowTicks: 3, ShrinkBelow: 0.3, ShrinkTicks: 5, Cooldown: 0}
}

func TestCrescePorSoDepoisDeAmostrasSeguidasComPressao(t *testing.T) {
	s := newSizer(testPolicy())

	got := []int{s.next(loaded), s.next(loaded), s.next(loaded)}

	if got[0] != 0 || got[1] != 0 || got[2] != 1 {
		t.Errorf("decisões %v, esperado [0 0 1]: só a terceira amostra seguida faz crescer", got)
	}
}

func TestUmaAmostraCalmaZeraAContagem(t *testing.T) {
	s := newSizer(testPolicy())

	s.next(loaded)
	s.next(loaded)
	s.next(calm) // quebra a sequência
	got := []int{s.next(loaded), s.next(loaded), s.next(loaded)}

	if got[0] != 0 || got[1] != 0 || got[2] != 1 {
		t.Errorf("decisões %v, esperado [0 0 1]: a contagem recomeçou depois da amostra calma", got)
	}
}

func TestPressaoSemEsperaNaoFazCrescer(t *testing.T) {
	s := newSizer(testPolicy())
	pronta := loaded
	pronta.Wait = time.Millisecond // há tarefa pronta, mas começa logo: mais workers não ajudam

	for range 20 {
		if s.next(pronta) != 0 {
			t.Fatal("cresceu sem que a espera passasse do limiar")
		}
	}
}

func TestEncolheSoDepoisDeMuitasAmostrasOciosas(t *testing.T) {
	s := newSizer(testPolicy())

	var got []int
	for range 5 {
		got = append(got, s.next(idle))
	}

	if got[3] != 0 || got[4] != -1 {
		t.Errorf("decisões %v, esperado encolher só na quinta amostra ociosa", got)
	}
}

func TestRespeitaOPisoEOTeto(t *testing.T) {
	s := newSizer(testPolicy())
	noTeto, noPiso := loaded, idle
	noTeto.Size = noTeto.Max
	noPiso.Size = noPiso.Min

	for range 30 {
		if s.next(noTeto) != 0 {
			t.Fatal("cresceu além do teto")
		}
		if s.next(noPiso) != 0 {
			t.Fatal("encolheu abaixo do piso")
		}
	}
}

func TestPausaDepoisDeMudarIgnoraAsAmostrasSeguintes(t *testing.T) {
	p := testPolicy()
	p.GrowTicks, p.Cooldown = 1, 2
	s := newSizer(p)

	got := []int{s.next(loaded), s.next(loaded), s.next(loaded), s.next(loaded)}

	if got[0] != 1 || got[1] != 0 || got[2] != 0 || got[3] != 1 {
		t.Errorf("decisões %v, esperado [1 0 0 1]: duas amostras de pausa depois de crescer", got)
	}
}

func TestCargaQueAlternaCheiaEVaziaNaoFazOscilar(t *testing.T) {
	s := newSizer(testPolicy())

	for i := range 200 {
		x := loaded
		if i%2 == 1 {
			x = idle
		}
		if d := s.next(x); d != 0 {
			t.Fatalf("amostra %d: decisão %d; a carga alternada não pode redimensionar", i, d)
		}
	}
}

func TestPoliticaZeradaUsaOPadraoEParcialPreencheOResto(t *testing.T) {
	if got := (Policy{}).withDefaults(); got != DefaultPolicy() {
		t.Errorf("Policy{} = %+v, esperado o padrão %+v", got, DefaultPolicy())
	}
	got := Policy{GrowTicks: 7}.withDefaults()
	d := DefaultPolicy()
	if got.GrowTicks != 7 || got.ShrinkTicks != d.ShrinkTicks || got.GrowWait != d.GrowWait || got.Cooldown != 0 {
		t.Errorf("parcial = %+v; esperado GrowTicks 7, o resto no padrão e Cooldown 0 (sem pausa)", got)
	}
}
