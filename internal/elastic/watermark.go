// watermark.go
// A marca d'água contígua de um fluxo (uma partição do Kafka): o maior prefixo de tarefas
// concluídas, sem buracos.
// Objetivo: o offset só pode avançar até onde TUDO antes dele terminou. Se a tarefa 7 termina
// antes da 5, a 7 fica anotada e a marca não se mexe; quando a 5 termina, a marca pula sobre
// 5, 6 e 7. Confirmar o offset da 7 antes da 5 perderia a 5 se o processo caísse agora.

package elastic

// watermark acompanha um fluxo. Não é seguro para uso concorrente: o Pool o protege com o seu
// mutex.
type watermark struct {
	issued uint64              // próxima sequência a entregar a quem submete
	next   uint64              // menor sequência ainda não concluída
	done   map[uint64]struct{} // concluídas à frente de next
}

func newWatermark() *watermark {
	return &watermark{done: make(map[uint64]struct{})}
}

// issue entrega a próxima sequência do fluxo, na ordem em que as tarefas foram submetidas.
func (w *watermark) issue() uint64 {
	seq := w.issued
	w.issued++
	return seq
}

// complete anota que seq terminou. Devolve true e a marca nova (todas as sequências <= upTo
// terminaram) quando o prefixo contíguo cresceu. Uma sequência repetida ou já coberta é ignorada.
func (w *watermark) complete(seq uint64) (upTo uint64, advanced bool) {
	if seq < w.next {
		return 0, false
	}
	if _, dup := w.done[seq]; dup {
		return 0, false
	}
	w.done[seq] = struct{}{}
	start := w.next
	for {
		if _, ok := w.done[w.next]; !ok {
			break
		}
		delete(w.done, w.next)
		w.next++
	}
	if w.next == start {
		return 0, false
	}
	return w.next - 1, true
}
