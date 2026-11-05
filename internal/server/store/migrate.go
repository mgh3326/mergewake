package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

//go:embed schema/*.sql
var schema embed.FS

type migration struct{ name, table, ddl, checksum string }

func migrations() []migration {
	entries, _ := schema.ReadDir("schema")
	var result []migration
	for _, entry := range entries {
		b, _ := schema.ReadFile("schema/" + entry.Name())
		ddl := string(b)
		digest := sha256.Sum256(b)
		result = append(result, migration{entry.Name(), strings.Fields(ddl)[5], ddl, hex.EncodeToString(digest[:])})
	}
	return result
}

const migrationTable = `CREATE TABLE IF NOT EXISTS mw_schema_migrations (
 name VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 checksum CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 applied_at DATETIME(6) NOT NULL
) ENGINE=InnoDB`

// Migrate is explicit and uses a dedicated connection for the advisory lock.
// Each file creates one table. An interrupted DDL can resume only after the
// actual information_schema definition matches the embedded migration.
func (s *Store) Migrate(ctx context.Context) error {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer c.Close()
	var locked int
	if err = c.QueryRowContext(ctx, `SELECT GET_LOCK(CONCAT('mw-migrate-',LEFT(SHA2(DATABASE(),256),32)),10)`).Scan(&locked); err != nil || locked != 1 {
		return ErrUnavailable
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), queryTimeout)
		defer cancel()
		_, _ = c.ExecContext(cleanup, `DO RELEASE_LOCK(CONCAT('mw-migrate-',LEFT(SHA2(DATABASE(),256),32)))`)
	}()
	if _, err = c.ExecContext(ctx, migrationTable); err != nil {
		return ErrUnavailable
	}
	if err = checkVersions(ctx, c, false); err != nil {
		return err
	}
	for _, m := range migrations() {
		var checksum, status string
		err = c.QueryRowContext(ctx, `SELECT checksum,status FROM mw_schema_migrations WHERE name=?`, m.name).Scan(&checksum, &status)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = c.ExecContext(ctx, `INSERT INTO mw_schema_migrations VALUES (?,?,'started',UTC_TIMESTAMP(6))`, m.name, m.checksum)
		} else if err == nil && (checksum != m.checksum || (status != "started" && status != "complete")) {
			return ErrSchema
		}
		if err != nil {
			return ErrUnavailable
		}
		if status != "complete" {
			if _, err = c.ExecContext(ctx, m.ddl); err != nil {
				return ErrSchema
			}
		}
		if err = validateTable(ctx, c, m); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `UPDATE mw_schema_migrations SET status='complete',applied_at=UTC_TIMESTAMP(6) WHERE name=? AND checksum=?`, m.name, m.checksum); err != nil {
			return ErrUnavailable
		}
	}
	_, err = c.ExecContext(ctx, `INSERT IGNORE INTO mw_capacity VALUES (1,6,4)`)
	if err != nil {
		return ErrUnavailable
	}
	return nil
}

type querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func checkVersions(ctx context.Context, q querier, complete bool) error {
	rows, err := q.QueryContext(ctx, `SELECT name,checksum,status FROM mw_schema_migrations`)
	if err != nil {
		return ErrSchema
	}
	defer rows.Close()
	known := map[string]string{}
	for _, m := range migrations() {
		known[m.name] = m.checksum
	}
	count := 0
	for rows.Next() {
		var name, sum, status string
		if rows.Scan(&name, &sum, &status) != nil {
			return ErrSchema
		}
		expected, knownName := known[name]
		if !knownName || expected != sum || (status != "started" && status != "complete") || (complete && status != "complete") {
			return ErrSchema
		}
		count++
	}
	if rows.Err() != nil {
		return ErrUnavailable
	}
	if complete && count != len(known) {
		return ErrSchema
	}
	return nil
}

func (s *Store) CheckSchema(ctx context.Context) error {
	if err := checkVersions(ctx, s.db, true); err != nil {
		return err
	}
	for _, m := range migrations() {
		if err := validateTable(ctx, s.db, m); err != nil {
			return err
		}
	}
	var reviews, fixes int
	if err := s.db.QueryRowContext(ctx, `SELECT review_slots,fix_slots FROM mw_capacity WHERE id=1`).Scan(&reviews, &fixes); err != nil {
		return ErrSchema
	}
	if reviews < 2 || fixes < 1 {
		return ErrSchema
	}
	return nil
}

var columnDef = regexp.MustCompile(`^ ([a-z_]+) ((?:BIGINT|INT)(?: UNSIGNED)?|MEDIUMBLOB|VARCHAR\([0-9]+\)|CHAR\([0-9]+\)|DATETIME\(6\)|JSON) (.+?)(?:,)?$`)
var indexDef = regexp.MustCompile(`^ (PRIMARY KEY|(?:UNIQUE )?KEY [a-z_]+) \(([^)]+)\),?$`)
var foreignDef = regexp.MustCompile(`^ FOREIGN KEY \(([^)]+)\) REFERENCES ([a-z_]+) \(([^)]+)\),?$`)

type column struct{ name, kind, nullable, collation, extra string }
type index struct {
	unique  int
	columns string
}
type shape struct {
	columns []column
	indexes map[string]index
	foreign []string
}

// The embedded DDL deliberately uses this small grammar. A new construct must
// extend the validator; it cannot silently bypass interrupted-DDL validation.
func expectedShape(m migration) (shape, error) {
	s := shape{indexes: map[string]index{}}
	for _, line := range strings.Split(m.ddl, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "CREATE TABLE") || strings.HasPrefix(line, ") ENGINE=") {
			continue
		}
		if a := columnDef.FindStringSubmatch(line); a != nil {
			c := column{name: a[1], kind: strings.ToLower(a[2]), nullable: "NO"}
			if !strings.Contains(a[3], "NOT NULL") {
				return shape{}, ErrSchema
			}
			if strings.Contains(a[2], "CHAR(") {
				if !strings.Contains(a[3], "CHARACTER SET ascii COLLATE ascii_bin") {
					return shape{}, ErrSchema
				}
				c.collation = "ascii_bin"
			}
			if strings.Contains(a[3], "AUTO_INCREMENT") {
				c.extra = "auto_increment"
			}
			s.columns = append(s.columns, c)
			continue
		}
		if a := indexDef.FindStringSubmatch(line); a != nil {
			name := strings.TrimPrefix(strings.TrimPrefix(a[1], "UNIQUE "), "KEY ")
			unique := 1
			if a[1] == "PRIMARY KEY" {
				name = "PRIMARY"
				unique = 0
			} else if strings.HasPrefix(a[1], "UNIQUE") {
				unique = 0
			}
			s.indexes[name] = index{unique, strings.ReplaceAll(a[2], " ", "")}
			continue
		}
		if a := foreignDef.FindStringSubmatch(line); a != nil {
			s.foreign = append(s.foreign, strings.ReplaceAll(a[1], " ", "")+"|"+a[2]+"|"+strings.ReplaceAll(a[3], " ", ""))
			continue
		}
		return shape{}, ErrSchema
	}
	if len(s.columns) == 0 {
		return shape{}, ErrSchema
	}
	sort.Strings(s.foreign)
	return s, nil
}

func validateTable(ctx context.Context, q querier, m migration) error {
	want, err := expectedShape(m)
	if err != nil {
		return err
	}
	var engine, collation string
	if err = q.QueryRowContext(ctx, `SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?`, m.table).Scan(&engine, &collation); err != nil {
		return ErrSchema
	}
	if engine != "InnoDB" || collation != "utf8mb4_bin" {
		return ErrSchema
	}
	rows, err := q.QueryContext(ctx, `SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COALESCE(COLLATION_NAME,''),EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, m.table)
	if err != nil {
		return ErrUnavailable
	}
	i := 0
	for rows.Next() {
		var got column
		if rows.Scan(&got.name, &got.kind, &got.nullable, &got.collation, &got.extra) != nil {
			rows.Close()
			return ErrSchema
		}
		if i >= len(want.columns) || got != want.columns[i] {
			rows.Close()
			return ErrSchema
		}
		i++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ErrUnavailable
	}
	if i != len(want.columns) {
		return ErrSchema
	}
	rows, err = q.QueryContext(ctx, `SELECT INDEX_NAME,NON_UNIQUE,GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? GROUP BY INDEX_NAME,NON_UNIQUE`, m.table)
	if err != nil {
		return ErrUnavailable
	}
	i = 0
	for rows.Next() {
		var name, columns string
		var nonunique int
		if rows.Scan(&name, &nonunique, &columns) != nil {
			rows.Close()
			return ErrSchema
		}
		if x, ok := want.indexes[name]; !ok || x != (index{nonunique, columns}) {
			rows.Close()
			return ErrSchema
		}
		i++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ErrUnavailable
	}
	if i != len(want.indexes) {
		return ErrSchema
	}
	rows, err = q.QueryContext(ctx, `SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ','),REFERENCED_TABLE_NAME,GROUP_CONCAT(REFERENCED_COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ',') FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND REFERENCED_TABLE_NAME IS NOT NULL GROUP BY CONSTRAINT_NAME,REFERENCED_TABLE_NAME`, m.table)
	if err != nil {
		return ErrUnavailable
	}
	var got []string
	for rows.Next() {
		var cols, table, refs string
		if rows.Scan(&cols, &table, &refs) != nil {
			rows.Close()
			return ErrSchema
		}
		got = append(got, cols+"|"+table+"|"+refs)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ErrUnavailable
	}
	sort.Strings(got)
	if fmt.Sprint(got) != fmt.Sprint(want.foreign) {
		return ErrSchema
	}
	return nil
}
