package commands

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
	"github.com/spf13/cobra"
)

// NewSchemaCommand creates the schema command group
func NewSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Schema registry operations",
		Long:  `Manage JSON schemas for message validation - register, list, get, and validate schemas.`,
	}

	cmd.AddCommand(newSchemaRegisterCommand())
	cmd.AddCommand(newSchemaListCommand())
	cmd.AddCommand(newSchemaGetCommand())
	cmd.AddCommand(newSchemaDeleteCommand())
	cmd.AddCommand(newSchemaValidateCommand())

	return cmd
}

// newSchemaRegisterCommand creates the schema register subcommand
func newSchemaRegisterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "register <schema-id> <schema-file>",
		Short: "Register a new JSON schema",
		Long:  `Register a new JSON schema or new version of an existing schema for message validation.`,
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			schemaID := args[0]
			schemaFile := args[1]

			// Read schema from file
			schemaBytes, err := os.ReadFile(schemaFile)
			if err != nil {
				return fmt.Errorf("failed to read schema file: %w", err)
			}

			// Validate JSON format
			var schemaJSON map[string]interface{}
			if err := json.Unmarshal(schemaBytes, &schemaJSON); err != nil {
				return fmt.Errorf("invalid JSON schema: %w", err)
			}

			// Get flags
			description, _ := cmd.Flags().GetString("description")
			tags, _ := cmd.Flags().GetStringSlice("tags")
			name, _ := cmd.Flags().GetString("name")

			if name == "" {
				name = schemaID // Use ID as name if not provided
			}

			return WithClient(cmd, func(c *client.ChronoQueueClient) error {
				outputs.PrintInfo(fmt.Sprintf("Registering schema: %s", schemaID))

				// Prepare metadata
				metadata := make(map[string]string)
				if len(tags) > 0 {
					for i, tag := range tags {
						metadata[fmt.Sprintf("tag_%d", i)] = tag
					}
				}

				// Prepare schema options
				options := client.SchemaOptions{
					Name:        name,
					Description: description,
					Content:     string(schemaBytes),
					ContentType: "json-schema",
					Metadata:    metadata,
				}

				// Call client method
				err := c.RegisterSchema(cmd.Context(), schemaID, options)
				if err != nil {
					outputs.PrintError(fmt.Sprintf("Failed to register schema: %v", err))
					outputs.PrintWarning("Note: This requires server-side schema service implementation")
					outputs.PrintInfo("You can still use schemas by setting schema_id and schema_version when posting messages")
					return err
				}

				outputs.PrintSuccess(fmt.Sprintf("Schema registered: %s", schemaID))
				return nil
			})
		},
	}

	cmd.Flags().StringP("description", "d", "", "Schema description")
	cmd.Flags().StringSliceP("tags", "t", []string{}, "Schema tags (comma-separated)")
	cmd.Flags().StringP("name", "n", "", "Schema name (defaults to schema ID)")

	return cmd
}

// newSchemaListCommand creates the schema list subcommand
func newSchemaListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all registered schemas",
		Long:  `List all registered schemas in the schema registry.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			activeOnly, _ := cmd.Flags().GetBool("active-only")
			prefix, _ := cmd.Flags().GetString("prefix")
			limit, _ := cmd.Flags().GetInt32("limit")

			return WithClient(cmd, func(c *client.ChronoQueueClient) error {
				outputs.PrintInfo(fmt.Sprintf("Listing schemas (prefix: %s, active only: %v)", prefix, activeOnly))

				// Call client method
				schemas, err := c.ListSchemas(cmd.Context(), prefix, limit, activeOnly)
				if err != nil {
					outputs.PrintError(fmt.Sprintf("Failed to list schemas: %v", err))
					outputs.PrintWarning("Note: This requires server-side schema service implementation")
					return err
				}

				// Print results
				formatter := outputs.NewOutputFormatter(outputs.OutputJSON)
				if err := formatter.Print(schemas); err != nil {
					return fmt.Errorf("failed to print schemas: %w", err)
				}

				outputs.PrintSuccess(fmt.Sprintf("Found %d schema(s)", len(schemas)))
				return nil
			})
		},
	}

	cmd.Flags().BoolP("active-only", "a", true, "Show only active schemas")
	cmd.Flags().StringP("prefix", "p", "", "Filter by schema ID prefix")
	cmd.Flags().Int32P("limit", "l", 100, "Maximum number of schemas to return")

	return cmd
}

// newSchemaGetCommand creates the schema get subcommand
func newSchemaGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <schema-id> [version]",
		Short: "Get a specific schema",
		Long:  `Get a specific schema by ID and optionally version. If version is omitted, returns the latest version.`,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			schemaID := args[0]
			var version int32 = 0 // 0 means latest

			if len(args) > 1 {
				v, err := fmt.Sscanf(args[1], "%d", &version)
				if err != nil || v != 1 {
					return fmt.Errorf("invalid version number: %s", args[1])
				}
			}

			return WithClient(cmd, func(c *client.ChronoQueueClient) error {
				if version == 0 {
					outputs.PrintInfo(fmt.Sprintf("Getting latest version of schema: %s", schemaID))
				} else {
					outputs.PrintInfo(fmt.Sprintf("Getting schema: %s version %d", schemaID, version))
				}

				// Call client method
				schema, err := c.GetSchema(cmd.Context(), schemaID, version)
				if err != nil {
					outputs.PrintError(fmt.Sprintf("Failed to get schema: %v", err))
					outputs.PrintWarning("Note: This requires server-side schema service implementation")
					return err
				}

				// Print results
				formatter := outputs.NewOutputFormatter(outputs.OutputJSON)
				if err := formatter.Print(schema); err != nil {
					return fmt.Errorf("failed to print schema: %w", err)
				}

				return nil
			})
		},
	}

	return cmd
}

// newSchemaDeleteCommand creates the schema delete subcommand
func newSchemaDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <schema-id> [version]",
		Short: "Delete a schema or schema version",
		Long:  `Delete a specific schema version or all versions of a schema. If version is omitted or 0, all versions are deleted.`,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			schemaID := args[0]
			var version int32 = 0 // 0 means all versions

			if len(args) > 1 {
				v, err := fmt.Sscanf(args[1], "%d", &version)
				if err != nil || v != 1 {
					return fmt.Errorf("invalid version number: %s", args[1])
				}
			}

			return WithClient(cmd, func(c *client.ChronoQueueClient) error {
				if version == 0 {
					outputs.PrintInfo(fmt.Sprintf("Deleting all versions of schema: %s", schemaID))
				} else {
					outputs.PrintInfo(fmt.Sprintf("Deleting schema: %s version %d", schemaID, version))
				}

				// Call client method
				err := c.DeleteSchema(cmd.Context(), schemaID, version)
				if err != nil {
					outputs.PrintError(fmt.Sprintf("Failed to delete schema: %v", err))
					outputs.PrintWarning("Note: This requires server-side schema service implementation")
					return err
				}

				if version == 0 {
					outputs.PrintSuccess(fmt.Sprintf("Schema deleted: %s (all versions)", schemaID))
				} else {
					outputs.PrintSuccess(fmt.Sprintf("Schema deleted: %s version %d", schemaID, version))
				}
				return nil
			})
		},
	}

	return cmd
}

// newSchemaValidateCommand creates the schema validate subcommand
func newSchemaValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate <schema-id> <json-file>",
		Short: "Validate JSON against a schema",
		Long:  `Validate a JSON file against a registered schema.`,
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			schemaID := args[0]
			jsonFile := args[1]

			// Read JSON from file
			jsonBytes, err := os.ReadFile(jsonFile)
			if err != nil {
				return fmt.Errorf("failed to read JSON file: %w", err)
			}

			// Validate JSON format
			var jsonData interface{}
			if err := json.Unmarshal(jsonBytes, &jsonData); err != nil {
				return fmt.Errorf("invalid JSON: %w", err)
			}

			version, _ := cmd.Flags().GetInt32("version")

			return WithClient(cmd, func(c *client.ChronoQueueClient) error {
				if version == 0 {
					outputs.PrintInfo(fmt.Sprintf("Validating JSON against latest version of schema: %s", schemaID))
				} else {
					outputs.PrintInfo(fmt.Sprintf("Validating JSON against schema: %s version %d", schemaID, version))
				}

				// Call client method
				err := c.ValidatePayload(cmd.Context(), schemaID, version, string(jsonBytes))
				if err != nil {
					outputs.PrintError(fmt.Sprintf("Validation failed: %v", err))
					if err.Error() == "schema validation requires server-side implementation (protobuf definitions and service methods)" {
						outputs.PrintWarning("Note: This requires server-side schema service implementation")
					}
					return err
				}

				outputs.PrintSuccess("Validation successful - payload matches schema")
				return nil
			})
		},
	}

	cmd.Flags().Int32P("version", "v", 0, "Schema version (0 = latest)")

	return cmd
}
