using System;
using System.Collections.Generic;
using System.CommandLine;
using System.IO;
using System.Threading.Tasks;
using SqlDataExtractor.Actors;
using SqlDataExtractor.Core;

namespace SqlDataExtractor;

public class Program
{
    private static readonly Dictionary<Database, DbActor> _dbActors = new()
    {
        { Database.PostgreSql, new PostgreSql() },
    };

    public static async Task<int> Main(string[] args)
    {
        var appCommand = new RootCommand("An application for extracting data from SQL-based databases.");
        Config.RegisterCommands(appCommand, _dbActors);
        appCommand.Name = Path.GetFileName(Environment.ProcessPath) ?? "";
        return await appCommand.InvokeAsync(args);
    }

}
