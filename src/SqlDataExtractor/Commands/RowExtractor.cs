using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqlDataExtractor.Actors;
using SqlDataExtractor.Core;

namespace SqlDataExtractor.Commands;

/// <summary>
/// Represents a command for extracting rows from a table whilst preserving referential integrity.
/// </summary>
public class RowExtractor : Command
{
    private const string SubFilePrefix = "row_dump_";
    private const string DestinationFileName = "row_dump";
    private readonly string _schemaName = string.Empty;
    private readonly string _rootTableName = string.Empty;
    private readonly string _whereFilter = string.Empty;
    private readonly int? _limit = null;

    public RowExtractor(DbActor dbActor, string connection, string schemaName, string rootTable, string whereFilter, int? limit)
        : base(dbActor, connection)
    {
        _schemaName = schemaName;
        _rootTableName = rootTable;
        _whereFilter = whereFilter;
        _limit = limit;
    }

    public override async Task<bool> RunAsync()
    {
        var schema = await DbActor.GetSchemaAsync(await GetConnectionAsync(), _schemaName);
        var isRootQueryFetched = false;
        var foreignColumns = new Queue<Column>();
        int dumpFileCount = 1;
        // a map of Table.FullName and its values that is used to prevent duplication
        var dataMap = new Dictionary<string, List<object[]>>();

        // // dump the loaded schema
        // using (var fs = new FileStream("schema.json", FileMode.Create, FileAccess.Write))
        // {
        //     System.Text.Json.JsonSerializer.Serialize(fs, schema);
        // }
        // return true;

        while (!isRootQueryFetched || foreignColumns.Count > 0)
        {
            if (!isRootQueryFetched)
            {
                var table = DbActor.GetTable(schema, _rootTableName);
                if (table == null)
                {
                    return false;
                }
                var results = await DbActor.GetQueryResultsAsync(await GetConnectionAsync(), _schemaName, _rootTableName, _whereFilter, _limit);
                AddForeignColumns(table, results, foreignColumns);
                dataMap.Add(table.Name, new List<object[]>(results));
                SaveResults(table, results, dumpFileCount);
                dumpFileCount++;
                isRootQueryFetched = true;
            }
            else
            {
                var foreignColumn = foreignColumns.Dequeue();
                var (foreignTable, position) = GetForeignColumnTableAndPosition(schema, foreignColumn);
                if (foreignTable == null || position < 0)
                {
                    return false;
                }
                if (foreignColumn.ForeignValues.Count == 0)
                {
                    continue;
                }
                var results = await GetForeignQueryResultsAsync(await GetConnectionAsync(), foreignColumn, foreignColumn.DataType);
                if (TryAddResults(dataMap, results, foreignColumn.ForeignTableName, position, out List<object[]> foreignValues))
                {
                    AddForeignColumns(schema, foreignColumn.ForeignTableName, foreignValues, foreignColumns);
                    SaveResults(foreignTable, foreignValues, dumpFileCount);
                    dumpFileCount++;
                }
            }
        }
        await (await GetConnectionAsync()).CloseAsync();
        JoinFiles(dumpFileCount, SubFilePrefix, DestinationFileName);
        return true;
    }

    private static void AddForeignColumns(Table table, List<object[]> results, Queue<Column> foreignColumns)
    {
        var foreignReferenceColumns = new List<Column>();
        for (int i = 0; i < table.Columns.Count; i++)
        {
            var column = table.Columns[i];
            if (column.IsForeignReference)
            {
                var newColumn = new Column()
                {
                    Name = column.Name,
                    TableName = column.TableName,
                    DataType = column.DataType,
                    IsNullable = column.IsNullable,
                    IsForeignReference = column.IsForeignReference,
                    ForeignTableName = column.ForeignTableName,
                    ForeignColumnName = column.ForeignColumnName,
                    ForeignValues = results.Select(row => row[i]).ToList(),
                };
                foreignReferenceColumns.Add(newColumn);
            }
        }
        // sort self-referencing columns higher (by giving it a lower comparison score) to increase the
        // chances that they have to share required values
        foreignReferenceColumns.Sort(
            (col1, col2) => col1.ForeignTableName == table.FullName ? -2 : col1.ForeignTableName.CompareTo(col2.ForeignTableName)
        );
        foreach (var column in foreignReferenceColumns)
        {
            foreignColumns.Enqueue(column);
        }
    }

    private static void AddForeignColumns(Schema schema, string tableFullName, List<object[]> results, Queue<Column> foreignColumns)
    {
        foreach (var table in schema.Tables)
        {
            if (table.FullName == tableFullName)
            {
                AddForeignColumns(table, results, foreignColumns);
                break;
            }
        }
    }

    private static bool TryAddResults(Dictionary<string, List<object[]>> dataMap, List<object[]> results, string mapKey, int position, out List<object[]> foreignValues)
    {
        foreignValues = new List<object[]>();
        var hasNewRows = false;
        bool rowExists;

        if (dataMap.ContainsKey(mapKey))
        {
            foreach (var row in results)
            {
                rowExists = false;

                foreach (var existingRow in dataMap[mapKey])
                {
                    if (existingRow[position].Equals(row[position]))
                    {
                        rowExists = true;
                        break;
                    }
                }
                if (!rowExists)
                {
                    foreignValues.Add(row);
                    dataMap[mapKey].Add(row);
                    hasNewRows = true;
                }
            }
        }
        else
        {
            foreignValues.AddRange(results);
            dataMap.Add(mapKey, results);
            hasNewRows = true;
        }
        return hasNewRows;
    }

    private static (Table?, int) GetForeignColumnTableAndPosition(Schema schema, Column foreignColumn)
    {
        foreach (var table in schema.Tables)
        {
            if (table.FullName == foreignColumn.ForeignTableName)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    var column = table.Columns[i];
                    if (column.Name == foreignColumn.ForeignColumnName)
                    {
                        return (table, i);
                    }
                }
                return (table, -1);
            }
        }
        return (null, -1);
    }

    private async Task<List<object[]>> GetForeignQueryResultsAsync(DbConnection connection, Column foreignColumn, string dataType)
    {
        var rows = new List<object[]>();
        var cmd = connection.CreateCommand();
        cmd.CommandText = string.Format(
            "SELECT * FROM {0} WHERE {1} IN {2};",
            foreignColumn.ForeignTableName,
            foreignColumn.ForeignColumnName,
            GetSet(foreignColumn.ForeignValues, dataType)
        );

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
        return rows;
    }

    /// <summary>
    /// Generates a set-like record with unique values from a list of values.
    /// </summary>
    private string GetSet(List<object> values, string dataType)
    {
        var uniqueValues = new HashSet<object>(values);
        var tupleBuilder = new StringBuilder();
        tupleBuilder.Append('(');

        for (int i = 0; i < uniqueValues.Count; i++)
        {
            var columnValue = uniqueValues.ElementAt(i);
            tupleBuilder.Append(i == 0 ? string.Empty : ", ");
            if (columnValue == null || columnValue.GetType() == typeof(System.DBNull))
            {
                tupleBuilder.Append("null");
            }
            else if (DbActor.IsQuotableType(dataType))
            {
                tupleBuilder.Append($"{DbActor.ValueOpeningQuote}{columnValue}{DbActor.ValueClosingQuote}");
            }
            else
            {
                tupleBuilder.Append(columnValue);
            }
        }
        tupleBuilder.Append(')');
        return tupleBuilder.ToString();
    }

    private void SaveResults(Table table, List<object[]> results, int dumpFileCount)
    {
        if (results.Count == 0)
        {
            return;
        }
        using var fs = new FileStream($"{SubFilePrefix}{dumpFileCount}.sql", FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(fs);
        var quotedColumnNames = table.Columns.Select(col => $"{DbActor.NameOpeningQuote}{col.Name}{DbActor.NameClosingQuote}").ToList();
        writer.Write($"INSERT INTO {table.FullName}({string.Join(", ", quotedColumnNames)}) VALUES\n");
        for (int i = 0; i < results.Count; i++)
        {
            writer.Write("    (");
            for (int j = 0; j < table.Columns.Count; j++)
            {
                var columnValue = results[i][j];
                writer.Write(j == 0 ? string.Empty : ", ");
                if (columnValue == null || columnValue.GetType() == typeof(System.DBNull))
                {
                    writer.Write("null");
                }
                else if (DbActor.IsQuotableType(table.Columns[j].DataType))
                {
                    writer.Write($"'{columnValue}'");
                }
                else
                {
                    writer.Write(columnValue);
                }
            }
            writer.Write(i == results.Count - 1 ? ")\n;\n" : "),\n");
        }
    }

}
