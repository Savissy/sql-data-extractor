using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqlDataExtractor.Core;

namespace SqlDataExtractor.Actors;

/// <summary>
/// Represents an object that performs actions for a specific database.
/// </summary>
public abstract class DbActor
{
    public virtual string DefaultSchema => string.Empty;
    public virtual char NameOpeningQuote => '"';
    public virtual char NameClosingQuote => '"';
    public virtual char ValueOpeningQuote => '\'';
    public virtual char ValueClosingQuote => '\'';
    protected virtual HashSet<string> QuotableTypes { get; } = new HashSet<string>();

    public abstract DbConnection GetConnection(string connection);

    public virtual bool IsQuotableType(string typeName)
        => QuotableTypes.Any(quotableTypeName => quotableTypeName.Equals(typeName, StringComparison.InvariantCultureIgnoreCase));

    public virtual async Task<Schema> GetSchemaAsync(DbConnection connection, string schemaName)
    {
        var tables = new List<Table>();
        var tableNames = await GetSchemaTableNamesAsync(connection, schemaName);

        foreach (var tableName in tableNames)
        {
            var columns = await GetTableColumnsAsync(connection, schemaName, tableName);
            var tableFullName = GetSchemaEntityFullName(schemaName, tableName);
            var table = new Table()
            {
                Name = tableName,
                FullName = tableFullName,
                Columns = columns,
            };
            await LoadTableForeignKeyConstraintsAsync(connection, schemaName, table);
            tables.Add(table);
        }
        var schema = new Schema()
        {
            Name = schemaName,
            Tables = tables,
        };
        return schema;
    }

    /// <summary>
    /// Retrieves the table with the given name from the schema.
    /// </summary>
    /// <param name="schema"></param>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public virtual Table? GetTable(Schema schema, string tableName)
    {
        return schema.Tables.Find(table => table.Name == tableName);
    }

    public virtual async Task<List<object[]>> GetQueryResultsAsync(DbConnection connection, string schemaName, string tableName, string whereFilter = "", int? limit = null)
    {
        var rows = new List<object[]>();
        var getTableElementsQuery = string.Format(
            "SELECT * FROM {0} {1} {2};",
            GetSchemaEntityFullName(schemaName, tableName),
            string.IsNullOrWhiteSpace(whereFilter) ? "" : $"WHERE {whereFilter}",
            limit.HasValue ? $"LIMIT {limit}" : ""
        );
        var cmd = connection.CreateCommand();
        cmd.CommandText = getTableElementsQuery;
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                if (reader.FieldCount > 0)
                {
                    var row = new object[reader.FieldCount];
                    reader.GetValues(row);
                    rows.Add(row);
                }
            }
        }
        await cmd.DisposeAsync();
        return rows;
    }

    public abstract Task<List<string>> GetSchemaDefinedTypesAsync(DbConnection connection, string schemaName);
    public abstract Task<List<string>> GetSchemaDefinedFunctionsAsync(DbConnection connection, string schemaName);
    public abstract Task<string> GetTableDefinitionAsync(DbConnection connection, string schemaName, Table table);
    public abstract Task<List<string>> GetTableIndexesAsync(DbConnection connection, string schemaName, Table table);
    public abstract Task<List<string>> GetTableConstraintsAsync(DbConnection connection, string schemaName, Table table);
    public abstract Task<List<string>> GetTableTriggersAsync(DbConnection connection, string schemaName, Table table);

    protected virtual string GetSchemaEntityFullName(string schema, string entityName, bool forceQuoteSchema = false, bool forceQuoteEntity = false)
    {
        var fullNameBuilder = new StringBuilder();

        if (schema != DefaultSchema && !string.IsNullOrWhiteSpace(schema))
        {
            if (forceQuoteSchema)
            {
                fullNameBuilder.Append($"{NameOpeningQuote}{schema}{NameClosingQuote}.");
            }
            else
            {
                fullNameBuilder.Append($"{schema}.");
            }
        }
        if (forceQuoteEntity || entityName.Any(ch => char.IsWhiteSpace(ch) || (char.IsLetter(ch) && char.IsUpper(ch))))
        {
            fullNameBuilder.Append($"{NameOpeningQuote}{entityName}{NameClosingQuote}");
        }
        else
        {
            fullNameBuilder.Append(entityName);
        }
        return fullNameBuilder.ToString();
    }

    protected abstract Task<List<string>> GetSchemaTableNamesAsync(DbConnection connection, string schemaName);

    protected abstract Task<List<Column>> GetTableColumnsAsync(DbConnection connection, string schemaName, string tableName);

    protected abstract Task LoadTableForeignKeyConstraintsAsync(DbConnection connection, string schemaName, Table table);

}
