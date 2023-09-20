using System;
using System.Collections.Generic;

namespace SqlDataExtractor.Core;

public class Schema
{
    public string Name { get; set; } = string.Empty;
    public List<Table> Tables { get; set; } = new List<Table>();
}

public class Table
{
    public string Name { get; set; } = string.Empty;
    public string FullName { get; set; } = string.Empty;
    public List<Column> Columns { get; set; } = new List<Column>();
}

public class Column
{
    public string Name { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public bool IsForeignReference { get; set; }
    public string ForeignTableName { get; set; } = string.Empty;
    public string ForeignColumnName { get; set; } = string.Empty;
    public List<object> ForeignValues { get; set; } = new List<object>();
}
