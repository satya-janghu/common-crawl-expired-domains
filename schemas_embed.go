package schemadata

import (
	"embed"
	"fmt"
	"path/filepath"
)

//go:embed schemas/*.sql
var sqlSchemas embed.FS

func ReadSQL(name string) (string, error) {
	data, err := sqlSchemas.ReadFile(filepath.Join("schemas", name))
	if err != nil {
		return "", fmt.Errorf("embedded schema not found: %s: %w", name, err)
	}
	return string(data), nil
}
