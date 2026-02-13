package introspection

import "testing"

func TestExtractAutoRandomColumns(t *testing.T) {
	tests := []struct {
		name      string
		createSQL string
		wantCols  map[string]bool
	}{
		{
			name: "single column auto_random",
			createSQL: "CREATE TABLE `t` (\n" +
				"  `id` bigint NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB",
			wantCols: map[string]bool{"id": true},
		},
		{
			name: "auto_random without precision",
			createSQL: "CREATE TABLE `t` (\n" +
				"  `id` bigint NOT NULL /*T![auto_rand] AUTO_RANDOM */,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB",
			wantCols: map[string]bool{"id": true},
		},
		{
			name: "multiple columns only one auto_random",
			createSQL: "CREATE TABLE `t` (\n" +
				"  `id` bigint NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n" +
				"  `name` varchar(20) NOT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB",
			wantCols: map[string]bool{"id": true},
		},
		{
			name: "auto_random_base comment should not match",
			createSQL: "CREATE TABLE `t` (\n" +
				"  `id` bigint NOT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB /*T![auto_rand_base] AUTO_RANDOM_BASE=30001 */",
			wantCols: map[string]bool{},
		},
		{
			name: "no auto_random",
			createSQL: "CREATE TABLE `t` (\n" +
				"  `id` bigint NOT NULL,\n" +
				"  `name` varchar(20)\n" +
				") ENGINE=InnoDB",
			wantCols: map[string]bool{},
		},
		{
			name: "auto_random in regular comment should not match",
			createSQL: "CREATE TABLE `t` (\n" +
				"  `note` varchar(20) COMMENT 'AUTO_RANDOM',\n" +
				"  PRIMARY KEY (`note`)\n" +
				") ENGINE=InnoDB",
			wantCols: map[string]bool{},
		},
		{
			name: "whitespace variations",
			createSQL: "CREATE TABLE `t` (\n" +
				"  `id` bigint NOT NULL /*T![auto_rand]  AUTO_RANDOM(5)  */ ,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB",
			wantCols: map[string]bool{"id": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractAutoRandomColumns(tt.createSQL)
			if len(got) != len(tt.wantCols) {
				t.Fatalf("expected %d columns, got %d", len(tt.wantCols), len(got))
			}
			for col := range tt.wantCols {
				if !got[col] {
					t.Fatalf("expected column %q to be auto_random", col)
				}
			}
		})
	}
}
