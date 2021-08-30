package sqlchemy

type SBaseBackend struct {
}

func (bb *SBaseBackend) GetTableSQL() string {
	return "SHOW TABLES"
}

func (bb *SBaseBackend) GetCreateSQL(ts ITableSpec) string {
	return ""
}
