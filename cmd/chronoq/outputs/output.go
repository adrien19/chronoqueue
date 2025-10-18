package outputs

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"

	"gopkg.in/yaml.v3"
)

// OutputFormat represents the output format
type OutputFormat string

const (
	OutputTable OutputFormat = "table"
	OutputJSON  OutputFormat = "json"
	OutputYAML  OutputFormat = "yaml"
)

// OutputFormatter handles formatting output in different formats
type OutputFormatter struct {
	format OutputFormat
}

// NewOutputFormatter creates a new output formatter
func NewOutputFormatter(format OutputFormat) *OutputFormatter {
	return &OutputFormatter{format: format}
}

// Print prints data in the specified format
func (f *OutputFormatter) Print(data interface{}) error {
	switch f.format {
	case OutputJSON:
		return f.printJSON(data)
	case OutputYAML:
		return f.printYAML(data)
	case OutputTable:
		fallthrough
	default:
		return f.printTable(data)
	}
}

// printJSON prints data as JSON
func (f *OutputFormatter) printJSON(data interface{}) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

// printYAML prints data as YAML
func (f *OutputFormatter) printYAML(data interface{}) error {
	encoder := yaml.NewEncoder(os.Stdout)
	defer encoder.Close()
	return encoder.Encode(data)
}

// printTable prints data as a table using tabwriter
func (f *OutputFormatter) printTable(data interface{}) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	if data == nil {
		fmt.Fprintln(w, "No data")
		return nil
	}

	v := reflect.ValueOf(data)
	t := reflect.TypeOf(data)

	// Handle pointers
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			fmt.Fprintln(w, "No data")
			return nil
		}
		v = v.Elem()
		t = t.Elem()
	}

	switch v.Kind() {
	case reflect.Slice:
		return f.renderSlice(w, v)
	case reflect.Struct:
		return f.renderStruct(w, v, t)
	default:
		fmt.Fprintf(w, "%v\n", data)
	}

	return nil
}

// renderSlice renders a slice as a table
func (f *OutputFormatter) renderSlice(w *tabwriter.Writer, v reflect.Value) error {
	if v.Len() == 0 {
		fmt.Fprintln(w, "No items")
		return nil
	}

	// Get the first element to determine structure
	firstElem := v.Index(0)
	if firstElem.Kind() == reflect.Ptr {
		firstElem = firstElem.Elem()
	}

	if firstElem.Kind() != reflect.Struct {
		// Simple slice - just print values
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i)
			fmt.Fprintf(w, "%v\n", elem.Interface())
		}
		return nil
	}

	// Struct slice - create table with struct fields as columns
	t := firstElem.Type()
	headers := make([]string, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.IsExported() {
			headers = append(headers, field.Name)
		}
	}

	// Print headers
	fmt.Fprintln(w, strings.Join(headers, "\t"))

	// Print rows
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}

		row := make([]string, 0)
		for j := 0; j < elem.NumField(); j++ {
			field := elem.Type().Field(j)
			if field.IsExported() {
				fieldValue := elem.Field(j)
				row = append(row, fmt.Sprintf("%v", fieldValue.Interface()))
			}
		}
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}

	return nil
}

// renderStruct renders a single struct as a key-value table
func (f *OutputFormatter) renderStruct(w *tabwriter.Writer, v reflect.Value, t reflect.Type) error {
	fmt.Fprintln(w, "Field\tValue")

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if field.IsExported() {
			fieldValue := v.Field(i)
			fmt.Fprintf(w, "%s\t%v\n", field.Name, fieldValue.Interface())
		}
	}

	return nil
}

// TruncateString truncates a string to the specified length
func TruncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// PrintSuccess prints a success message in green
func PrintSuccess(msg string) {
	fmt.Printf("✓ %s\n", msg)
}

// PrintError prints an error message in red
func PrintError(msg string) {
	fmt.Printf("✗ %s\n", msg)
}

// PrintInfo prints an info message
func PrintInfo(msg string) {
	fmt.Printf("ℹ %s\n", msg)
}

// PrintWarning prints a warning message in yellow
func PrintWarning(msg string) {
	fmt.Printf("⚠ %s\n", msg)
}
