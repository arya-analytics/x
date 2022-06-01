package alamos

import (
	"encoding/json"
	"io"
)

type Reporter interface {
	Report() Report
}

type Report map[string]interface{}

func AttachReport(exp Experiment, key string, rep Report) {
	if exp == nil {
		return
	}
	exp.attachReport(key, rep)
}

func AttachReporter(exp Experiment, key string, report Reporter) {
	if exp == nil {
		return
	}
	exp.attachReporter(key, report)
}

// JSON writes the report as JSON as bytes.
func (r Report) JSON() ([]byte, error) {
	return json.Marshal(r)
}

// WriteJSON writes the report as JSON to the given writer.
func (r Report) WriteJSON(w io.Writer) error {
	return json.NewEncoder(w).Encode(r)
}

func (r Report) String() string {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// Report implements Experiment.
func (e *experiment) Report() Report {
	report := make(map[string]interface{})
	for k, v := range e.measurements {
		report[k] = v.Report()
	}
	for k, v := range e.children {
		report[k] = v.Report()
	}
	for k, v := range e.reports {
		report[k] = v
	}
	for k, v := range e.reporters {
		report[k] = v.Report()
	}
	return report
}

func (e *experiment) attachReport(key string, r Report) {
	e.reports[key] = r
}

func (e *experiment) attachReporter(key string, r Reporter) {
	e.reporters[key] = r
}
