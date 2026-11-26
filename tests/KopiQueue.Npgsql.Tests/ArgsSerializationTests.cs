using System.Text.Json;
using System.Text.Json.Serialization;
using KopiQueue.Migrations;
using KopiQueue.Serialization;
using KopiQueue.TestInfrastructure;

namespace KopiQueue.Npgsql.Tests;

/// <summary>Args serialised by a <see cref="JsonSerializerContext"/> through PostgreSQL and back (plan task 1.10: design §9, NFR-COMP-06).</summary>
public sealed class ArgsSerializationTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Context_serialised_args_read_back_from_jsonb()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = "j_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        var serializer = new SystemTextJsonArgsSerializer(OrderJsonContext.Default);
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = serializer });
        var args = new ShipOrderArgs(42, "Zoë <zoe@example.com>", [new OrderLine("ab-1", 2), new OrderLine("cd-2", 1)], DateTimeOffset.Parse("2026-11-26T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture));

        var single = await client.InsertAsync(args, cancellationToken: Ct);
        var many = await client.InsertManyAsync([new(args)], cancellationToken: Ct);
        await client.InsertManyFastAsync([new(args)], cancellationToken: Ct);
        var fast = await ((Drivers.IKopiDriver)driver).GetExecutor().JobGetByIdAsync(many[0].Job.Id + 1, Ct);

        // jsonb re-formats the text ("{"orderId": 42, ...}"), and it must still read back.
        foreach (var json in new[] { single.Job.ArgsJson, many[0].Job.ArgsJson, fast!.ArgsJson })
        {
            Assert.Contains("\"orderId\": 42", json, StringComparison.Ordinal);
            var read = Assert.IsType<ShipOrderArgs>(serializer.Deserialize(json, typeof(ShipOrderArgs)));
            Assert.Equal((args.OrderId, args.Recipient, args.PromisedBy), (read.OrderId, read.Recipient, read.PromisedBy));
            Assert.Equal(args.Lines, read.Lines);
        }
    }
}

[JobKind("ship_order")]
public sealed record ShipOrderArgs(int OrderId, string Recipient, IReadOnlyList<OrderLine> Lines, DateTimeOffset PromisedBy) : IJobArgs;

public sealed record OrderLine(string Sku, int Quantity);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(ShipOrderArgs))]
internal sealed partial class OrderJsonContext : JsonSerializerContext;
