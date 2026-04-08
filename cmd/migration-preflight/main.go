// Command migration-preflight validates supported migration actions before the migrate CLI is invoked.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/LerianStudio/matcher/internal/bootstrap"
)

var (
	preflightMigrationUp      = bootstrap.PreflightMigrationUp
	preflightMigrationDownOne = bootstrap.PreflightMigrationDownOne
	preflightMigrationGoto    = bootstrap.PreflightMigrationGoto
	errTargetVersionRequired  = errors.New("target is required for goto")
	errInvalidTargetVersion   = errors.New("invalid target version")
)

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stderr))
}

func run(args []string, getenv func(string) string, stderr io.Writer) int {
	var (
		dsnRaw           string
		action           string
		targetVersionRaw string
	)

	flags := flag.NewFlagSet("migration-preflight", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&dsnRaw, "dsn", "", "database connection string")
	flags.StringVar(&action, "action", "up", "migration action: up, down, goto")
	flags.StringVar(&targetVersionRaw, "target", "", "target version for goto")

	if err := flags.Parse(args); err != nil {
		return 1
	}

	dsn := strings.TrimSpace(dsnRaw)
	if dsn == "" {
		dsn = strings.TrimSpace(getenv("DATABASE_URL"))
	}

	if dsn == "" {
		fmt.Fprintln(stderr, "dsn is required (or set DATABASE_URL)")
		return 1
	}

	ctx := context.Background()

	var err error

	switch action {
	case "up":
		err = preflightMigrationUp(ctx, dsn)
	case "down":
		err = preflightMigrationDownOne(ctx, dsn)
	case "goto":
		targetVersion, targetErr := parseTargetVersion(targetVersionRaw)
		if targetErr != nil {
			fmt.Fprintln(stderr, targetErr)
			return 1
		}

		err = preflightMigrationGoto(ctx, dsn, targetVersion)
	default:
		fmt.Fprintf(stderr, "unsupported action %q\n", action)
		return 1
	}

	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	return 0
}

func parseTargetVersion(raw string) (int, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return 0, errTargetVersionRequired
	}

	parsed, err := strconv.ParseInt(target, 10, 0)
	if err != nil {
		return 0, fmt.Errorf("%w %q", errInvalidTargetVersion, raw)
	}

	return int(parsed), nil
}
