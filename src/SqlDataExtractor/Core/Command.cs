using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;
using SqlDataExtractor.Actors;

namespace SqlDataExtractor.Core;

/// <summary>
/// Represents a SQL data extraction tool.
/// </summary>
public abstract class Command
{
    protected readonly DbActor DbActor;
    private DbConnection? _connection;
    private readonly AsyncRetryPolicy<DbConnection?> _getConnectionPolicyAsync;
    private const int MaxRetryCount = 5;
    private readonly string _connectionParameters;
    private readonly ConnectionState[] _faultyConnectionStates = new[] { ConnectionState.Closed, ConnectionState.Broken };

    protected async Task<DbConnection> GetConnectionAsync()
    {
        var result = await _getConnectionPolicyAsync
            .ExecuteAsync(async () =>
            {
                if (_connection is null || _faultyConnectionStates.Contains(_connection.State))
                {
                    try
                    {
                        _connection = DbActor.GetConnection(_connectionParameters);
                        if (_connection is not null)
                        {
                            await _connection.OpenAsync();
                        }
                    }
                    catch (Exception)
                    {
                    }
                }
                return _connection;
            });
        if (result is null || _faultyConnectionStates.Contains(result.State))
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Write("ERROR: ");
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine("Failed to create a connection to the database.");
            Environment.Exit(1);
        }
        return result!;
    }

    public Command(DbActor dbActor, string connection)
    {
        DbActor = dbActor;
        _connectionParameters = connection;
        _getConnectionPolicyAsync = Policy
            .HandleResult<DbConnection?>(connection => connection is null || _faultyConnectionStates.Contains(connection.State))
            .WaitAndRetryAsync(
                MaxRetryCount,
                retryAttempt => TimeSpan.FromSeconds(10 * retryAttempt),
                (retryConnectionResult, timeSpan, retryAttempt, ctx) =>
                {
                    Console.ForegroundColor = ConsoleColor.Blue;
                    Console.Write("INFO: ");
                    Console.ForegroundColor = ConsoleColor.Gray;
                    Console.WriteLine($"Retrying {retryAttempt}/{MaxRetryCount}. Database connection state is `{retryConnectionResult?.Result?.State ?? ConnectionState.Closed}`.");
                }
            );
    }

    public abstract Task<bool> RunAsync();

    protected static void JoinFiles(int count, string subFilePrefix, string fileName, bool isReversed = true)
    {
        using var fs = new FileStream($"{fileName}.sql", FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(fs);
        var start = isReversed ? count - 1 : 0;
        var end = isReversed ? 0 : count;
        var increment = isReversed ? -1 : 1;

        for (int i = start; i != end; i += increment)
        {
            var subFileName = $"{subFilePrefix}{i}.sql";

            if (File.Exists(subFileName))
            {
                writer.WriteLine(File.ReadAllText(subFileName));
                File.Delete(subFileName);
            }
        }
    }

}
