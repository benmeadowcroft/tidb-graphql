package introspection

// EffectiveLocalColumns returns the ordered local key mapping for a relationship.
func (r Relationship) EffectiveLocalColumns() []string {
	if len(r.LocalColumns) > 0 {
		return append([]string(nil), r.LocalColumns...)
	}
	if r.LocalColumn == "" {
		return nil
	}
	return []string{r.LocalColumn}
}

// EffectiveRemoteColumns returns the ordered remote key mapping for a relationship.
func (r Relationship) EffectiveRemoteColumns() []string {
	if len(r.RemoteColumns) > 0 {
		return append([]string(nil), r.RemoteColumns...)
	}
	if r.RemoteColumn == "" {
		return nil
	}
	return []string{r.RemoteColumn}
}

// EffectiveJunctionLocalFKColumns returns the ordered junction local FK mapping.
func (r Relationship) EffectiveJunctionLocalFKColumns() []string {
	if len(r.JunctionLocalFKColumns) > 0 {
		return append([]string(nil), r.JunctionLocalFKColumns...)
	}
	if r.JunctionLocalFK == "" {
		return nil
	}
	return []string{r.JunctionLocalFK}
}

// EffectiveJunctionRemoteFKColumns returns the ordered junction remote FK mapping.
func (r Relationship) EffectiveJunctionRemoteFKColumns() []string {
	if len(r.JunctionRemoteFKColumns) > 0 {
		return append([]string(nil), r.JunctionRemoteFKColumns...)
	}
	if r.JunctionRemoteFK == "" {
		return nil
	}
	return []string{r.JunctionRemoteFK}
}

func firstColumnOrEmpty(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	return cols[0]
}

// EffectiveColumnNames returns ordered FK column names for a junction FK mapping.
func (j JunctionFKInfo) EffectiveColumnNames() []string {
	if len(j.ColumnNames) > 0 {
		return append([]string(nil), j.ColumnNames...)
	}
	if j.ColumnName == "" {
		return nil
	}
	return []string{j.ColumnName}
}

// EffectiveReferencedColumns returns ordered referenced columns for a junction FK mapping.
func (j JunctionFKInfo) EffectiveReferencedColumns() []string {
	if len(j.ReferencedColumns) > 0 {
		return append([]string(nil), j.ReferencedColumns...)
	}
	if j.ReferencedColumn == "" {
		return nil
	}
	return []string{j.ReferencedColumn}
}

func columnNamesFromColumns(cols []Column) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = col.Name
	}
	return result
}
