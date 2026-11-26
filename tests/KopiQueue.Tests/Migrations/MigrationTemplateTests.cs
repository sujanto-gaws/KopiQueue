using KopiQueue.Migrations;

namespace KopiQueue.Tests.Migrations;

public sealed class MigrationTemplateTests
{
    [Theory]
    [InlineData("CREATE TABLE {{schema}}{{prefix}}job (id int);", "\"app\".", "river_", "CREATE TABLE \"app\".river_job (id int);")]
    [InlineData("CREATE TABLE {{schema}}{{prefix}}job (id int);", "", "kopi_", "CREATE TABLE kopi_job (id int);")]
    [InlineData("{{ prefix }}a, {{prefix}}b", "", "p_", "p_a, p_b")]
    [InlineData("no placeholders", "\"x\".", "p_", "no placeholders")]
    public void Substitutes_schema_and_prefix(string sql, string schema, string prefix, string expected)
    {
        Assert.Equal(expected, MigrationTemplate.Render(sql, schema, prefix));
    }

    [Fact]
    public void Unknown_placeholders_are_rejected()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => MigrationTemplate.Render("SELECT {{shema}}x", "", "p_"));
        Assert.Contains("{{shema}}", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("public")]
    [InlineData("App_1")]
    [InlineData("_x")]
    public void Accepts_valid_schema_names(string? schema)
    {
        Assert.Equal(schema, SchemaIdentifiers.ValidateSchema(schema));
    }

    [Theory]
    [InlineData("")]
    [InlineData("1abc")]
    [InlineData("a b")]
    [InlineData("a\"; DROP TABLE x; --")]
    [InlineData("a.b")]
    [InlineData("ä")]
    public void Rejects_unsafe_schema_names(string schema)
    {
        Assert.Throws<ArgumentException>(() => SchemaIdentifiers.ValidateSchema(schema));
    }

    [Fact]
    public void Rejects_schema_names_longer_than_63_characters()
    {
        Assert.NotNull(SchemaIdentifiers.ValidateSchema(new string('s', 63)));
        Assert.Throws<ArgumentException>(() => SchemaIdentifiers.ValidateSchema(new string('s', 64)));
    }

    [Theory]
    [InlineData("river_")]
    [InlineData("kopi_")]
    [InlineData("x")]
    public void Accepts_valid_prefixes(string prefix)
    {
        Assert.Equal(prefix, SchemaIdentifiers.ValidatePrefix(prefix));
    }

    [Theory]
    [InlineData("")]
    [InlineData("River_")]
    [InlineData("9x_")]
    [InlineData("a-b")]
    [InlineData("x; DROP")]
    [InlineData("abcdefghijklmnopqrstuvwxyz_abcdefg")]
    public void Rejects_unsafe_prefixes(string prefix)
    {
        Assert.Throws<ArgumentException>(() => SchemaIdentifiers.ValidatePrefix(prefix));
    }
}
