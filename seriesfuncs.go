package loom

import (
	"context"
	"fmt"
	"strings"
)

// A deployment that splits its database roles — Migrate as the schema
// owner, the serving process as a role with DML rights only — cannot
// create or drop a @retain series' day partitions from the serving
// process: Postgres checks CREATE on the schema even for CREATE TABLE IF
// NOT EXISTS on a partition that exists. So Migrate, running as the
// owner, also creates two SECURITY DEFINER functions per service, and
// the serving process creates and expires partitions through them:
//
//   - loom_series_partition_<service>(tbl, day) creates one day's
//     partition of one of the service's @retain series, for a day inside
//     that series' retention and at most seriesFuncFutureDays ahead;
//   - loom_series_maintain_<service>(ahead) creates yesterday through
//     ahead days (clamped to 0..seriesFuncMaxAhead) for every @retain
//     series and drops the partitions past retention.
//
// The tables and their retention are written into the function bodies at
// Migrate, so a caller names nothing but a day count: it cannot reach
// another table or drop a partition retention still keeps. Search path
// and time zone are pinned, so the partition bounds match the ones
// maintainSeriesPartitions writes on a UTC session. A single-role
// deployment never needs them; the serving process uses them whenever
// they exist and otherwise runs the DDL itself, as before.

const (
	seriesFuncMaxAhead   = 60
	seriesFuncFutureDays = 400
)

// seriesFuncName is the schema-local name of the service's partition
// function of kind ("partition" or "maintain"). Service names are
// identifiers already; anything else is folded to '_' so the name is
// always a plain lower-case identifier.
func (c *Client) seriesFuncName(kind string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(c.reg.Service) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	name := "loom_series_" + kind + "_" + b.String()
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

// haveSeriesFuncs reports whether Migrate created the partition
// functions this client's schema should use. Once seen they are assumed
// to stay; until then every call looks again (one catalog lookup).
func (c *Client) haveSeriesFuncs(ctx context.Context) (bool, error) {
	if c.seriesFuncs.Load() {
		return true, nil
	}
	var ok bool
	if err := c.db.QueryRow(ctx, `SELECT to_regprocedure($1) IS NOT NULL`,
		c.seriesFuncName("maintain")+"(integer)").Scan(&ok); err != nil {
		return false, fmt.Errorf("loom: series functions: %w", err)
	}
	if ok {
		c.seriesFuncs.Store(true)
	}
	return ok, nil
}

// migrateSeriesFuncs (re)creates the service's partition functions,
// owned by the role running Migrate, for every @retain series on plain
// Postgres. A schema with no @retain series gets none.
func (c *Client) migrateSeriesFuncs(ctx context.Context) error {
	var tables []string
	var retains []string
	for _, ss := range sortedSeries(c.series) {
		if ss.def.RetainDays > 0 && ss.def.PartitionDDL != "" {
			tables = append(tables, "'"+strings.ReplaceAll(ss.def.Table, "'", "''")+"'")
			retains = append(retains, fmt.Sprint(ss.def.RetainDays))
		}
	}
	if len(tables) == 0 {
		return nil
	}
	var schema string
	if err := c.db.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return err
	}
	partition, maintain := c.seriesFuncName("partition"), c.seriesFuncName("maintain")
	decl := fmt.Sprintf(`tables text[] := ARRAY[%s]::text[];
  retains int[] := ARRAY[%s]::int[];`, strings.Join(tables, ", "), strings.Join(retains, ", "))
	settings := fmt.Sprintf(`SECURITY DEFINER SET search_path = %s, pg_temp SET TimeZone = 'UTC'`, quoteIdent(schema))
	stmts := []string{
		fmt.Sprintf(`CREATE OR REPLACE FUNCTION %[1]s(tbl text, day date) RETURNS void
LANGUAGE plpgsql %[2]s AS $loom$
DECLARE
  %[3]s
  i int := array_position(tables, tbl);
BEGIN
  IF i IS NULL THEN
    RAISE EXCEPTION 'loom: %% is not a @retain series of this service', tbl;
  END IF;
  IF day < current_date - retains[i] OR day > current_date + %[4]d THEN
    RAISE EXCEPTION 'loom: %% is outside the partition window of %%', day, tbl;
  END IF;
  EXECUTE format('CREATE TABLE IF NOT EXISTS %%I PARTITION OF %%I FOR VALUES FROM (%%L) TO (%%L)',
    left(tbl, 53) || '_p' || to_char(day, 'YYYYMMDD'), tbl, day, day + 1);
END
$loom$`, partition, settings, decl, seriesFuncFutureDays),
		fmt.Sprintf(`CREATE OR REPLACE FUNCTION %[1]s(ahead int) RETURNS void
LANGUAGE plpgsql %[2]s AS $loom$
DECLARE
  %[3]s
  t int;
  d int;
  part text;
  pday date;
BEGIN
  ahead := least(greatest(ahead, 0), %[4]d);
  FOR t IN 1 .. array_length(tables, 1) LOOP
    FOR d IN -1 .. ahead LOOP
      PERFORM %[5]s(tables[t], current_date + d);
    END LOOP;
    FOR part IN
      SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
      WHERE i.inhparent = tables[t]::regclass AND c.relname ~ '_p[0-9]{8}$'
    LOOP
      BEGIN
        pday := to_date(right(part, 8), 'YYYYMMDD');
      EXCEPTION WHEN others THEN
        CONTINUE;
      END;
      IF pday < current_date - retains[t] THEN
        EXECUTE format('DROP TABLE IF EXISTS %%I', part);
      END IF;
    END LOOP;
  END LOOP;
END
$loom$`, maintain, settings, decl, seriesFuncMaxAhead, partition),
		fmt.Sprintf(`GRANT EXECUTE ON FUNCTION %s(text, date), %s(integer) TO PUBLIC`, partition, maintain),
	}
	for _, q := range stmts {
		if _, err := c.db.Exec(ctx, q); err != nil {
			return fmt.Errorf("loom: series partition functions: %w", err)
		}
	}
	c.seriesFuncs.Store(true)
	return nil
}
