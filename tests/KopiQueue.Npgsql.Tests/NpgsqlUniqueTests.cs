using KopiQueue.TestInfrastructure.Integration;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// The shared unique-insert suite on PostgreSQL, against River (plan task 4.2: FR-UNQ-06): a job a River producer
/// inserted is a duplicate of the same job inserted by KopiQueue.
/// </summary>
public sealed class NpgsqlUniqueTests(NpgsqlIntegrationFixture db)
    : UniqueTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>
{
    [Fact]
    public async Task A_job_a_River_producer_inserted_is_a_duplicate_of_the_same_job_from_KopiQueue()
    {
        await using var schema = await Fixture.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        long riverJob;
        await using (var cmd = Fixture.DataSource.CreateCommand(
            $"""
            INSERT INTO {schema.Name}.river_job (args, kind, max_attempts, queue, state, unique_key, unique_states)
            VALUES ($2::jsonb, 'golden_tagged', 25, 'default', 'available', $1, B'11110101')
            RETURNING id
            """))
        {
            cmd.Parameters.Add(new NpgsqlParameter { Value = Convert.FromHexString(FieldTagsKey) });
            cmd.Parameters.Add(new NpgsqlParameter { Value = """{"region":"eu","customer":{"b":"x","a":1},"note":"from go"}""" });
            riverJob = (long)(await cmd.ExecuteScalarAsync(Ct))!;
        }

        var client = new KopiClient(driver);
        var result = await client.InsertAsync(
            new GoldenTaggedArgs("eu", new GoldenCustomer("x", 1), "from dotnet"), new InsertOptions { Unique = new() { ByArgs = true } }, Ct);

        Assert.True(result.UniqueSkippedAsDuplicate);
        Assert.Equal(riverJob, result.Job.Id);
        Assert.Contains("from go", result.Job.ArgsJson, StringComparison.Ordinal);
    }
}
