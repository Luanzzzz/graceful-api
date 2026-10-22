// Pacote config carrega o arquivo .env da raiz do projeto.
package config

import (
	"os"
	"strings"
)

// LoadDotEnv lê KEY=VALUE de path.
// Linha vazia e comentário (#) são ignorados.
// Variável que já existe no ambiente não é sobrescrita.
// Arquivo ausente não é erro: o processo pode receber DATABASE_URL de fora.
func LoadDotEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
}
