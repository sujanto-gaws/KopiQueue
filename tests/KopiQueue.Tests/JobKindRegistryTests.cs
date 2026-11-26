namespace KopiQueue.Tests;

/// <summary>Kinds from <see cref="JobKindAttribute"/> and the kind registry (plan task 1.5: FR-DEF-01, FR-DEF-06).</summary>
public sealed class JobKindRegistryTests
{
    [Fact]
    public void GetKind_reads_the_attribute()
    {
        Assert.Equal("send_email", JobKindRegistry.GetKind<SendEmailArgs>());
#pragma warning disable CA2263 // The Type overload is under test.
        Assert.Equal("send_email", JobKindRegistry.GetKind(typeof(SendEmailArgs)));
#pragma warning restore CA2263
        Assert.Equal("Struct.Kind", JobKindRegistry.GetKind<StructArgs>());
    }

    [Fact]
    public void GetKind_accepts_any_kind_of_1_to_127_characters()
    {
        Assert.Equal(new string('k', 127), JobKindRegistry.GetKind<LongestKindArgs>());
        Assert.Equal("x", JobKindRegistry.GetKind<ShortestKindArgs>());
    }

    [Fact]
    public void A_missing_attribute_is_a_configuration_error()
    {
        var ex = Assert.Throws<KopiQueueConfigurationException>(() => JobKindRegistry.GetKind<NoKindArgs>());

        Assert.Contains(nameof(NoKindArgs), ex.Message, StringComparison.Ordinal);
        Assert.Contains("[JobKind]", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_attribute_is_not_inherited()
    {
        Assert.Throws<KopiQueueConfigurationException>(() => JobKindRegistry.GetKind<DerivedArgs>());
    }

    [Theory]
    [InlineData(typeof(EmptyKindArgs), "0 characters long")]
    [InlineData(typeof(TooLongKindArgs), "128 characters long")]
    [InlineData(typeof(NullKindArgs), "null")]
    public void An_invalid_kind_is_a_configuration_error(Type argsType, string expected)
    {
        var ex = Assert.Throws<KopiQueueConfigurationException>(() => JobKindRegistry.GetKind(argsType));

        Assert.Contains(argsType.Name, ex.Message, StringComparison.Ordinal);
        Assert.Contains(expected, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void GetKind_rejects_a_type_that_is_not_job_args()
    {
        Assert.Throws<ArgumentException>(() => JobKindRegistry.GetKind(typeof(string)));
        Assert.Throws<ArgumentNullException>(() => JobKindRegistry.GetKind(null!));
    }

    [Fact]
    public void Register_maps_kinds_to_types_and_is_idempotent()
    {
        var registry = new JobKindRegistry();

        Assert.Equal("send_email", registry.Register<SendEmailArgs>());
#pragma warning disable CA2263 // The Type overload is under test.
        Assert.Equal("send_email", registry.Register(typeof(SendEmailArgs)));
#pragma warning restore CA2263
        registry.Register<StructArgs>();

        Assert.Equal(["Struct.Kind", "send_email"], registry.Kinds.Order(StringComparer.Ordinal));
        Assert.True(registry.TryGetType("send_email", out var type));
        Assert.Equal(typeof(SendEmailArgs), type);
        Assert.False(registry.TryGetType("Send_Email", out _));
    }

    [Fact]
    public void Two_types_with_the_same_kind_are_a_configuration_error()
    {
        var registry = new JobKindRegistry();
        registry.Register<SendEmailArgs>();

        var ex = Assert.Throws<KopiQueueConfigurationException>(() => registry.Register<OtherSendEmailArgs>());

        Assert.Contains("'send_email'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(SendEmailArgs), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(OtherSendEmailArgs), ex.Message, StringComparison.Ordinal);
        Assert.True(registry.TryGetType("send_email", out var type));
        Assert.Equal(typeof(SendEmailArgs), type);
    }

    [Fact]
    public void Registries_are_independent()
    {
        var first = new JobKindRegistry();
        first.Register<SendEmailArgs>();
        var second = new JobKindRegistry();

        Assert.Equal("send_email", second.Register<OtherSendEmailArgs>());
        Assert.Empty(new JobKindRegistry().Kinds);
    }

    [JobKind("send_email")]
    private sealed record SendEmailArgs(string To) : IJobArgs;

    [JobKind("send_email")]
    private sealed record OtherSendEmailArgs(string Address) : IJobArgs;

    [JobKind("Struct.Kind")]
    private readonly record struct StructArgs(int N) : IJobArgs;

    [JobKind("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk")]
    private sealed record LongestKindArgs : IJobArgs;

    [JobKind("x")]
    private sealed record ShortestKindArgs : IJobArgs;

    [JobKind("")]
    private sealed record EmptyKindArgs : IJobArgs;

    [JobKind("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk")]
    private sealed record TooLongKindArgs : IJobArgs;

    [JobKind(null!)]
    private sealed record NullKindArgs : IJobArgs;

    private sealed record NoKindArgs : IJobArgs;

    [JobKind("base")]
    private record BaseArgs : IJobArgs;

    private sealed record DerivedArgs : BaseArgs;
}
