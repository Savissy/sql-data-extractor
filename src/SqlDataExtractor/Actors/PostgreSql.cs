using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using SqlDataExtractor.Core;

namespace SqlDataExtractor.Actors;

public class PostgreSql : DbActor
{
    // source: https://docs.exasol.com/db/latest/sql_references/basiclanguageelements.htm
    private const string REGULAR_IDENTIFIER = "\\p{L}[\\p{L}\\p{Mn}\\p{Mc}\\p{Nd}\\p{Pc}\\p{Cf}\u00B7]*";
    private const string DELIMITED_IDENTIFIER = "\"(?:\"\"|[^\"\\.])*\"";
    private const string SQL_IDENTIFIER = $"(?:{REGULAR_IDENTIFIER}|{DELIMITED_IDENTIFIER})";
    private const string TABLE_IDENTIFIER = $"{SQL_IDENTIFIER}\\.{SQL_IDENTIFIER}";
    private static readonly Regex FOREIGN_KEY_PATTERN = new Regex(string.Format("^FOREIGN KEY \\(({0})\\) REFERENCES ({1})\\(({0})\\)", SQL_IDENTIFIER, TABLE_IDENTIFIER));
    private const char Constraint_Type_Foreign_Key = 'f';
    public override string DefaultSchema { get => "public"; }

    public override DbConnection GetConnection(string connection)
    {
        return new NpgsqlConnection(connection);
    }

    public PostgreSql()
    {
        QuotableTypes.Add("text");
        QuotableTypes.Add("character varying");
        QuotableTypes.Add("character");
        QuotableTypes.Add("citext");
        QuotableTypes.Add("json");
        QuotableTypes.Add("jsonb");
        QuotableTypes.Add("name");
        QuotableTypes.Add("uuid");
        QuotableTypes.Add("timestamp with time zone");
        QuotableTypes.Add("timestamp without time zone");
        QuotableTypes.Add("time with time zone");
        QuotableTypes.Add("time without time zone");
        QuotableTypes.Add("date");
        QuotableTypes.Add("interval");
        QuotableTypes.Add("ARRAY");
        QuotableTypes.Add("USER-DEFINED");
    }

    public override async Task<List<string>> GetSchemaDefinedTypesAsync(DbConnection connection, string schemaName)
    {
        var typeDefinitions = new List<string>();
        typeDefinitions.AddRange(await GetSchemaDefinedEnumTypesAsync(connection, schemaName));
        return typeDefinitions;
    }

    private async Task<List<string>> GetSchemaDefinedEnumTypesAsync(DbConnection connection, string schemaName)
    {
        var definitions = new List<string>();
        var typeNames = new List<string>();
        using (var cmd = new NpgsqlCommand("SELECT typname FROM pg_type WHERE typnamespace = $1::regnamespace AND typbyval = TRUE AND typcategory IN ('E')", (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName },
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var name = reader.GetString(0);
                    typeNames.Add(name);
                }
            }
        }
        foreach (var typeName in typeNames)
        {
            var enumValues = new List<string>();
            var enumFullName = GetSchemaEntityFullName(schemaName, typeName);
            using (var cmd = new NpgsqlCommand("SELECT enumlabel FROM pg_enum WHERE enumtypid = $1::regtype ORDER BY enumsortorder;", (NpgsqlConnection)connection)
            {
                Parameters =
            {
                new() { Value = enumFullName },
            }
            })
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    if (reader.IsOnRow)
                    {
                        var enumLabel = reader.GetString(0);
                        enumValues.Add($"{ValueOpeningQuote}{enumLabel}{ValueClosingQuote}");
                    }
                }
            }
            definitions.Add($"CREATE TYPE {enumFullName} AS ENUM (\n\t{string.Join(",\n\t", enumValues)}\n);");
        }
        return definitions;
    }

    public override async Task<List<string>> GetSchemaDefinedFunctionsAsync(DbConnection connection, string schemaName)
    {
        var definitions = new List<string>();
        using (var cmd = new NpgsqlCommand(
            @"SELECT pg_proc.oid::regprocedure::text, pg_proc.prorettype::regtype::text, pg_proc.prosrc, pgl.lanname
            FROM pg_proc
            LEFT OUTER JOIN pg_language pgl ON pgl.oid = pg_proc.prolang
            WHERE pg_proc.pronamespace = $1::regnamespace AND pg_proc.prokind IN ('f')",
            (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName },
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var name = reader.GetString(0);
                    var returnType = reader.GetString(1);
                    var body = reader.GetString(2);
                    var language = reader.GetString(3);
                    definitions.Add($"CREATE OR REPLACE FUNCTION {name}\nRETURNS {returnType} AS $$\n{body}\n$$ LANGUAGE {language};");
                }
            }
        }
        return definitions;
    }

    public override async Task<string> GetTableDefinitionAsync(DbConnection connection, string schemaName, Table table)
    {
        var definitionBuilder = new StringBuilder();
        definitionBuilder.Append($"CREATE TABLE {table.FullName} (\n");
        using (var cmd = new NpgsqlCommand(
            @"SELECT column_name, udt_name, is_nullable, column_default, is_identity, identity_generation
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position;",
            (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName },
                new() { Value = table.Name },
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var name = reader.GetString(0);
                    var dataType = reader.GetString(1);
                    var nullable = reader.GetString(2).ToLower() == "yes" ? "NULL" : "NOT NULL";
                    var defaultValue = reader.IsDBNull(3) ? null : reader.GetString(3);
                    var isIdentity = (reader.IsDBNull(4) ? "NO" : reader.GetString(4)).ToLower() == "yes";
                    var identityGeneration = reader.IsDBNull(5) ? string.Empty : reader.GetString(5);

                    definitionBuilder.Append($"\t{name} {dataType} {nullable}");
                    if (defaultValue is not null)
                    {
                        definitionBuilder.Append($" DEFAULT {defaultValue}");
                    }
                    if (isIdentity)
                    {
                        definitionBuilder.Append($" GENERATED {identityGeneration} AS IDENTITY");
                    }
                    definitionBuilder.Append(",\n");
                }
            }
        }
        definitionBuilder.Append(");");
        return definitionBuilder.ToString();
    }

    public override async Task<List<string>> GetTableIndexesAsync(DbConnection connection, string schemaName, Table table)
    {
        var definitions = new List<string>();
        using (var cmd = new NpgsqlCommand("SELECT indexdef FROM pg_catalog.pg_indexes WHERE schemaname = $1 AND tablename = $2", (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName },
                new() { Value = table.Name },
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var name = reader.GetString(0);
                    definitions.Add(name);
                }
            }
        }
        return definitions;
    }

    public override async Task<List<string>> GetTableConstraintsAsync(DbConnection connection, string schemaName, Table table)
    {
        var definitions = new List<string>();
        using (var cmd = new NpgsqlCommand(
            @"SELECT conname, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE connamespace = $1::regnamespace AND conrelid = $2::regclass",
            (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName },
                new() { Value = table.FullName },
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var name = reader.GetString(0);
                    var definition = reader.GetString(1);
                    definitions.Add($"ALTER TABLE {table.FullName} ADD CONSTRAINT {name} {definition};");
                }
            }
        }
        return definitions;
    }

    public override async Task<List<string>> GetTableTriggersAsync(DbConnection connection, string schemaName, Table table)
    {
        var definitions = new List<string>();
        using (var cmd = new NpgsqlCommand(
            @"SELECT trigger_name, action_timing, event_manipulation, action_orientation, action_statement
            FROM information_schema.triggers
            WHERE event_object_schema = $1 AND event_object_table = $2
            ORDER BY action_order;",
            (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName },
                new() { Value = table.FullName },
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var name = reader.GetString(0);
                    var actionTiming = reader.GetString(1);
                    var eventManipulation = reader.GetString(2);
                    var orientation = reader.GetString(3);
                    var action = reader.GetString(4);
                    definitions.Add($"CREATE TRIGGER {name} {actionTiming} {eventManipulation} ON {table.FullName} FOR EACH {orientation} {action};");
                }
            }
        }
        return definitions;
    }

    protected override async Task<List<string>> GetSchemaTableNamesAsync(DbConnection connection, string schemaName)
    {
        var rows = new List<string>();
        using (var cmd = new NpgsqlCommand("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1", (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName}
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var name = reader.GetString(0);
                    rows.Add(name);
                }
            }
        }
        return rows;
    }

    protected override async Task<List<Column>> GetTableColumnsAsync(DbConnection connection, string schemaName, string tableName)
    {
        var rows = new List<Column>();
        using (var cmd = new NpgsqlCommand(
            @"SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position;",
            (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName },
                new() { Value = tableName },
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var column = new Column()
                    {
                        Name = reader.GetString(0),
                        TableName = tableName,
                        DataType = reader.GetString(1),
                        IsNullable = reader.GetString(2).ToLower() == "yes",
                    };
                    rows.Add(column);
                }
            }
        }
        return rows;
    }

    protected override async Task LoadTableForeignKeyConstraintsAsync(DbConnection connection, string schemaName, Table table)
    {
        var getConstraintsQuery = string.Format(
            "SELECT pg_get_constraintdef(oid) AS constraint_def, contype FROM pg_constraint WHERE connamespace = $1::regnamespace AND conrelid = $2::regclass;"
        );
        var foreignKeyConstraints = new List<Tuple<string, string, string>>();
        using (var cmd = new NpgsqlCommand(getConstraintsQuery, (NpgsqlConnection)connection)
        {
            Parameters =
            {
                new() { Value = schemaName },
                new() { Value = table.FullName },
            }
        })
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.IsOnRow)
                {
                    var definition = reader.GetString(0);
                    var constraintType = reader.GetChar(1);

                    if (constraintType == Constraint_Type_Foreign_Key)
                    {
                        var fkPatternMatch = FOREIGN_KEY_PATTERN.Match(definition);
                        if (fkPatternMatch != null)
                        {
                            var column = fkPatternMatch.Groups[1].Value;
                            var foreignTable = fkPatternMatch.Groups[2].Value;
                            var foreignColumn = fkPatternMatch.Groups[3].Value;

                            foreignKeyConstraints.Add(Tuple.Create(column, foreignTable, foreignColumn));
                        }
                    }
                }
            }
        }

        foreach (var foreignKeyConstraint in foreignKeyConstraints)
        {
            foreach (var column in table.Columns)
            {
                if (column.Name == foreignKeyConstraint.Item1)
                {
                    column.IsForeignReference = true;
                    column.ForeignTableName = foreignKeyConstraint.Item2;
                    column.ForeignColumnName = foreignKeyConstraint.Item3;
                    break;
                }
            }
        }
    }

}
