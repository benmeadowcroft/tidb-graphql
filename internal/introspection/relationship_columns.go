package introspection

// EffectiveLocalColumns returns the ordered local key mapping for a relationship.
func (r Relationship) EffectiveLocalColumns() []string {
	return append([]string(nil), r.LocalColumns...)
}

// EffectiveRemoteColumns returns the ordered remote key mapping for a relationship.
func (r Relationship) EffectiveRemoteColumns() []string {
	return append([]string(nil), r.RemoteColumns...)
}

// EffectiveJunctionLocalFKColumns returns the ordered junction local FK mapping.
func (r Relationship) EffectiveJunctionLocalFKColumns() []string {
	return append([]string(nil), r.JunctionLocalFKColumns...)
}

// EffectiveJunctionRemoteFKColumns returns the ordered junction remote FK mapping.
func (r Relationship) EffectiveJunctionRemoteFKColumns() []string {
	return append([]string(nil), r.JunctionRemoteFKColumns...)
}

func firstColumnOrEmpty(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	return cols[0]
}

// EffectiveColumnNames returns ordered FK column names for a junction FK mapping.
func (j JunctionFKInfo) EffectiveColumnNames() []string {
	return append([]string(nil), j.ColumnNames...)
}

// EffectiveReferencedColumns returns ordered referenced columns for a junction FK mapping.
func (j JunctionFKInfo) EffectiveReferencedColumns() []string {
	return append([]string(nil), j.ReferencedColumns...)
}

func columnNamesFromColumns(cols []Column) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = col.Name
	}
	return result
}
