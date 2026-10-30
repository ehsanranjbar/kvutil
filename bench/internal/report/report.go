package report

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Generate reads the result directories and writes the report (report.md, report.html, charts/
// and the CSV files) to out.
func Generate(out string, dirs []string) error {
	if len(dirs) == 0 {
		return errors.New("no result directory")
	}
	var runs []*Run
	names := map[string]int{}
	for _, d := range dirs {
		run, err := LoadRun(d)
		if err != nil {
			return err
		}
		// Two runs from directories of the same name get distinct labels.
		if names[run.Name]++; names[run.Name] > 1 {
			run.Name = fmt.Sprintf("%s (%d)", run.Name, names[run.Name])
		}
		runs = append(runs, run)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	doc := Build(runs)
	if err := doc.WriteFiles(out); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "report: %s, %s\n", filepath.Join(out, "report.md"), filepath.Join(out, "report.html"))
	return nil
}
