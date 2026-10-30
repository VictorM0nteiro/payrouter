// pool.go
// O pool elástico de workers: roda tarefas em paralelo entre Min e Max goroutines, sem nunca
// rodar duas tarefas da mesma chave ao mesmo tempo nem fora de ordem, e avisa até onde cada
// fluxo terminou sem buracos.
// Objetivo: o paralelismo do processor vem de mensagens independentes chegando juntas; o limite
// vem do participante externo. O pool só pode crescer e encolher se isso não custar a ordem por
// agregado, e se o offset nunca for confirmado à frente de uma mensagem não concluída.
//
// Como as duas garantias valem:
//   - Exclusividade e ordem por chave: uma chave presente em keys tem exatamente uma tarefa na
//     fila de prontas ou rodando; as seguintes esperam na fila da própria chave e são liberadas
//     uma a uma, na ordem de submissão. Não importa qual worker roda a tarefa, então redimensionar
//     não reordena nada.
//   - Marca d'água: cada fluxo numera as tarefas na submissão e só avisa (OnWatermark) o prefixo
//     contíguo concluído (watermark.go).
//
// O pool não reenvia nada: uma tarefa que retorna terminou, com ou sem erro. Repetir um erro
// transitório é assunto da própria tarefa. Pânico numa tarefa não é recuperado.

package elastic