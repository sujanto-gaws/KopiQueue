using System.Net;
using System.Net.Sockets;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// A TCP proxy that can stop forwarding while keeping its sockets open, like a hung load balancer or an expired NAT
/// entry: the client sees neither FIN nor RST (spike 0.7, section G).
/// </summary>
internal sealed class BlackholeProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _cts = new();
    private readonly List<TcpClient> _sockets = [];
    private readonly Task _accept;
    private volatile bool _blackholed;

    public BlackholeProxy(string host, int port)
    {
        _listener.Start();
        _accept = AcceptAsync(host, port);
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    /// <summary>While set, bytes are read but never forwarded, in both directions.</summary>
    public bool Blackholed
    {
        get => _blackholed;
        set => _blackholed = value;
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _listener.Stop();
        lock (_sockets)
        {
            foreach (var socket in _sockets)
            {
                socket.Dispose();
            }
        }

        try
        {
            await _accept;
        }
        catch (OperationCanceledException)
        {
        }
        catch (SocketException)
        {
        }

        _cts.Dispose();
    }

    private async Task AcceptAsync(string host, int port)
    {
        while (!_cts.IsCancellationRequested)
        {
            var client = await _listener.AcceptTcpClientAsync(_cts.Token);
            var upstream = new TcpClient();
            await upstream.ConnectAsync(host, port, _cts.Token);
            lock (_sockets)
            {
                _sockets.AddRange([client, upstream]);
            }

            _ = PumpAsync(client.GetStream(), upstream.GetStream());
            _ = PumpAsync(upstream.GetStream(), client.GetStream());
        }
    }

    private async Task PumpAsync(NetworkStream from, NetworkStream to)
    {
        var buffer = new byte[64 * 1024];
        try
        {
            while (true)
            {
                var read = await from.ReadAsync(buffer, _cts.Token);
                if (read == 0)
                {
                    break;
                }

                while (_blackholed)
                {
                    await Task.Delay(50, _cts.Token);
                }

                await to.WriteAsync(buffer.AsMemory(0, read), _cts.Token);
            }
        }
        catch (Exception e) when (e is OperationCanceledException or IOException or ObjectDisposedException or SocketException)
        {
        }
    }
}
