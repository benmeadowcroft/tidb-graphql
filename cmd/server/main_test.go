package main

import "testing"

func TestResolveRoleSchemaTargets(t *testing.T) {
	tests := []struct {
		name      string
		roles     []string
		include   []string
		exclude   []string
		max       int
		want      []string
		expectErr bool
	}{
		{
			name:    "empty include defaults to wildcard",
			roles:   []string{"viewer", "admin"},
			include: []string{},
			exclude: []string{},
			max:     10,
			want:    []string{"admin", "viewer"},
		},
		{
			name:    "include and exclude filters apply",
			roles:   []string{"app_admin", "app_viewer", "ops_admin"},
			include: []string{"app_*"},
			exclude: []string{"*_admin"},
			max:     10,
			want:    []string{"app_viewer"},
		},
		{
			name:      "max cap exceeded fails",
			roles:     []string{"a", "b", "c"},
			include:   []string{"*"},
			exclude:   []string{},
			max:       2,
			expectErr: true,
		},
		{
			name:      "no matching roles fails",
			roles:     []string{"viewer"},
			include:   []string{"admin*"},
			exclude:   []string{},
			max:       10,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveRoleSchemaTargets(tt.roles, tt.include, tt.exclude, tt.max)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("length mismatch: got %d want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("mismatch at %d: got %q want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
