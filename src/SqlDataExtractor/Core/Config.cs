using System.Collections.Generic;
using System.CommandLine;
using SqlDataExtractor.Actors;
using SqlDataExtractor.Commands;

namespace SqlDataExtractor.Core;

public static class Config
{
    public static void RegisterCommands(RootCommand rootCommand, Dictionary<Database, DbActor> dbActors)
    {
        RegisterRowExtractorCommand(rootCommand, dbActors);
        RegisterSchemaExtractorCommand(rootCommand, dbActors);
    }

    private static void RegisterRowExtractorCommand(RootCommand rootCommand, Dictionary<Database, DbActor> dbActors)
    {
        var dbOption = new Option<Database>(
            name: "--db",
            getDefaultValue: () => default,
            description: "The type of SQL database."
        );
        var connectionOption = new Option<string>(
            name: "--connection",
            description: "The database connection string."
        );
        var schemaOption = new Option<string>(
            name: "--schema",
            getDefaultValue: () => "",
            description: "The database schema containing the table."
        );
        var tableOption = new Option<string>(
            name: "--table",
            description: "The table in the schema to fetch the intial rows from."
        );
        var whereOption = new Option<string>(
            name: "--where",
            getDefaultValue: () => string.Empty,
            description: "The table's row filtering expression."
        );
        var limitOption = new Option<int?>(
            name: "--limit",
            getDefaultValue: () => null,
            description: "The maximum number of rows to fetch from the table. Leave this unset to have no limit."
        );
        var rowExtractionCommand = new System.CommandLine.Command(
            name: "extract-rows",
            description: "Extracts rows from a table whilst preserving referential integrity."
        );
        rowExtractionCommand.AddOption(dbOption);
        rowExtractionCommand.AddOption(connectionOption);
        rowExtractionCommand.AddOption(schemaOption);
        rowExtractionCommand.AddOption(tableOption);
        rowExtractionCommand.AddOption(whereOption);
        rowExtractionCommand.AddOption(limitOption);
        rowExtractionCommand.SetHandler(async (database, connection, schema, table, whereFilter, limit) =>
        {
            var dbActor = dbActors[database];
            var rowExtractor = new RowExtractor(dbActor, connection, schema, table, whereFilter, limit);
            await rowExtractor.RunAsync();
        }, dbOption, connectionOption, schemaOption, tableOption, whereOption, limitOption);
        rootCommand.AddCommand(rowExtractionCommand);
    }

    private static void RegisterSchemaExtractorCommand(RootCommand rootCommand, Dictionary<Database, DbActor> dbActors)
    {
        var dbOption = new Option<Database>(
            name: "--db",
            getDefaultValue: () => default,
            description: "The type of SQL database."
        );
        var connectionOption = new Option<string>(
            name: "--connection",
            description: "The database connection string."
        );
        var schemaOption = new Option<string>(
            name: "--schema",
            getDefaultValue: () => "",
            description: "The database schema to extract."
        );
        var schemaExtractionCommand = new System.CommandLine.Command(
            name: "extract-schema",
            description: "Extracts user-defined types, functions, tables, indices, and constraints from a table."
        );
        schemaExtractionCommand.AddOption(dbOption);
        schemaExtractionCommand.AddOption(connectionOption);
        schemaExtractionCommand.AddOption(schemaOption);
        schemaExtractionCommand.SetHandler(async (database, connection, schema) =>
        {
            var dbActor = dbActors[database];
            var schemaExtractor = new SchemaExtractor(dbActor, connection, schema);
            await schemaExtractor.RunAsync();
        }, dbOption, connectionOption, schemaOption);
        rootCommand.AddCommand(schemaExtractionCommand);
    }

}
