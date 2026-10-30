// behavior_test.go
// Testes da escolha de comportamento do participante simulado, sem rede e sem Docker.
// Objetivo: garantir que o prefixo do end_to_end_id força o comportamento certo, que um
// prefixo desconhecido (ou ausente) cai no padrão, e que o nome inválido é recusado em vez de
// virar um padrão silencioso.

package participant

import "testing"

func TestResolve(t *testing.T) {
	tests := []struct {
		name       string
		endToEndID string
		def        Behavior
		want       Behavior
	}{
		{"prefixo_late_forca_late", "late:abc", Accept, Late},
		{"prefixo_crash_forca_crash", "crash:1", Accept, Crash},
		{"prefixo_ignora_maiusculas", "REJECT:x", Accept, Reject},
		{"sem_dois_pontos_usa_o_padrao", "e2e-123", Slow, Slow},
		{"prefixo_desconhecido_usa_o_padrao", "nope:abc", Drop, Drop},
		{"vazio_usa_o_padrao", "", Accept, Accept},
		{"so_dois_pontos_usa_o_padrao", ":abc", Reject, Reject},
		{"so_o_primeiro_prefixo_conta", "accept:late:x", Drop, Accept},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Resolve(tc.endToEndID, tc.def); got != tc.want {
				t.Errorf("Resolve(%q, %q) = %q, esperado %q", tc.endToEndID, tc.def, got, tc.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	for _, b := range All {
		got, err := Parse(string(b))
		if err != nil || got != b {
			t.Errorf("Parse(%q) = %q, %v", b, got, err)
		}
	}
	if got, err := Parse("  Late "); err != nil || got != Late {
		t.Errorf("Parse com espaços e maiúsculas = %q, %v; esperado late", got, err)
	}
	for _, bad := range []string{"", "acept", "timeout", "429"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) deveria falhar", bad)
		}
	}
}

func TestOsSeisComportamentosEstaoRegistrados(t *testing.T) {
	if len(All) != 7 {
		t.Errorf("comportamentos = %d, esperado 7: %v", len(All), All)
	}
}
