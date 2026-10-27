// reconcile.go
// Caso de uso da conciliação: varre as tarefas vencidas, consulta o status da instrução no
// participante e aplica a resposta, com backoff e jitter entre as tentativas.
// Objetivo: resolver o unknown SEM reenviar. Consultar o status é a única operação segura
// depois de um timeout: se o participante diz accepted ou rejected, a instrução é resolvida a
// partir da resposta dele; se não sabe (not_found, timeout, erro), a consulta é registrada e a
// tarefa volta à fila mais tarde. Cada tarefa é reivindicada com um lease, então vários
// processors nunca consultam a mesma, e uma queda no meio só adia a tarefa até o lease vencer.
package app