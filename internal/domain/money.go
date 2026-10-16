// money.go
// Tipo Money do domínio: valor monetário em centavos, inteiro de 64 bits.
// Objetivo: garantir pela construção a regra "dinheiro nunca é float". Nenhuma camada do
// projeto representa valor com ponto flutuante; aqui nasce o único tipo que carrega valor.

package domain

// Money é um valor em centavos. 15000 significa R$ 150,00.
type Money int64

// NewMoney cria um Money a partir de centavos. Rejeita zero e negativos, porque uma
// instrução de pagamento sempre move um valor positivo.
func NewMoney(cents int64) (Money, error) {
	if cents <= 0 {
		return 0, ErrInvalidAmount
	}
	return Money(cents), nil
}

// Cents devolve o valor em centavos.
func (m Money) Cents() int64 {
	return int64(m)
}