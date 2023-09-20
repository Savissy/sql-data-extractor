using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using SqlDataExtractor.Actors;
using SqlDataExtractor.Core;

namespace SqlDataExtractor.Commands;

/// <summary>
/// Represents a command for extracting user-defined types, functions, tables, indices, and constraints from a table.
/// </summary>
public class SchemaExtractor : Command
{
    private const string SubFilePrefix = "schema_dump_";
    private const string DestinationFileName = "schema_dump";
    private readonly string _schemaName = string.Empty;

    public SchemaExtractor(DbActor dbActor, string connection, string schemaName)
        : base(dbActor, connection)
    {
        _schemaName = schemaName;
    }

    public override async Task<bool> RunAsync()
    {
        var schema = await DbActor.GetSchemaAsync(await GetConnectionAsync(), _schemaName);
        int dumpFileCount = 1;
        var schemaDefinedTypes = await DbActor.GetSchemaDefinedTypesAsync(await GetConnectionAsync(), schema.Name);
        var schemaDefinedFunctions = await DbActor.GetSchemaDefinedFunctionsAsync(await GetConnectionAsync(), schema.Name);

        SaveExpressions(schemaDefinedTypes, dumpFileCount);
        dumpFileCount++;
        SaveExpressions(schemaDefinedFunctions, dumpFileCount);
        dumpFileCount++;
        foreach (var tablePosition in GetLinearTablePositions(schema))
        {
            var tableDefinition = await DbActor.GetTableDefinitionAsync(await GetConnectionAsync(), schema.Name, schema.Tables[tablePosition]);
            var tableIndexes = await DbActor.GetTableIndexesAsync(await GetConnectionAsync(), schema.Name, schema.Tables[tablePosition]);
            var tableTriggers = await DbActor.GetTableTriggersAsync(await GetConnectionAsync(), schema.Name, schema.Tables[tablePosition]);
            var tableConstraints = await DbActor.GetTableConstraintsAsync(await GetConnectionAsync(), schema.Name, schema.Tables[tablePosition]);
            SaveExpression(tableDefinition, dumpFileCount);
            dumpFileCount++;
            SaveExpressions(tableIndexes, dumpFileCount);
            dumpFileCount++;
            SaveExpressions(tableTriggers, dumpFileCount);
            dumpFileCount++;
            SaveExpressions(tableConstraints, dumpFileCount);
            dumpFileCount++;
        }
        JoinFiles(
            count: dumpFileCount,
            subFilePrefix: SubFilePrefix,
            fileName: DestinationFileName,
            isReversed: false);
        return true;
    }

    private static IEnumerable<int> GetLinearTablePositions(Schema schema)
    {
        var tableFullNames = schema.Tables.Select(table => table.FullName).OrderBy(fullName => fullName);
        var tablePositionMap = new Dictionary<string, Tuple<int, Table>>(
            schema.Tables.Select((table, id) => KeyValuePair.Create(table.FullName, Tuple.Create(id, table)))
        );
        var orderedTables = new List<string>();

        foreach (var tableFullName in tableFullNames)
        {
            var pendingTables = new List<string>();
            AddTableWithReferencedTables(ref orderedTables, tableFullName, ref tablePositionMap);
        }

        return orderedTables.Select(tableFullName => tablePositionMap[tableFullName].Item1);
    }

    private static void AddTableWithReferencedTables(ref List<string> orderedTables, string tableFullName, ref Dictionary<string, Tuple<int, Table>> tablePositionMap)
    {
        if (orderedTables.Contains(tableFullName))
        {
            return;
        }
        foreach (var column in tablePositionMap[tableFullName].Item2.Columns.Where(column => column.IsForeignReference).OrderBy(column => column.ForeignTableName))
        {
            if (orderedTables.Contains(column.ForeignTableName) || tableFullName == column.ForeignTableName)
            {
                continue;
            }
            AddTableWithReferencedTables(ref orderedTables, column.ForeignTableName, ref tablePositionMap);
        }
        if (orderedTables.Contains(tableFullName))
        {
            throw new NotSupportedException("Circular dependency detected");
        }
        orderedTables.Add(tableFullName);
    }

    private static void SaveExpressions(List<string> expressions, int dumpFileCount)
    {
        if (expressions.Count == 0)
        {
            return;
        }
        using var fs = new FileStream($"{SubFilePrefix}{dumpFileCount}.sql", FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(fs);
        for (int i = 0; i < expressions.Count; i++)
        {
            writer.WriteLine(expressions[i]);
        }
    }

    private static void SaveExpression(string expression, int dumpFileCount)
    {
        using var fs = new FileStream($"{SubFilePrefix}{dumpFileCount}.sql", FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(fs);
        writer.WriteLine(expression);
    }

}
