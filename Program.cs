using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Security.Cryptography;

var builder = WebApplication.CreateBuilder(args);
var port = Environment.GetEnvironmentVariable("PORT") ?? "5050";
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
var app = builder.Build();
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(20) });
var accounts = new AccountStore(Environment.GetEnvironmentVariable("DATABASE_URL"));
await accounts.Initialize();
var hub = new ChatHub(accounts);
app.MapGet("/", () => Results.Ok(new { name = "SigmaChat server", version = "6.0", status = "online", accountsConfigured = accounts.Configured }));
app.MapGet("/health", () => Results.Ok("ok"));
app.MapPost("/api/register", async (AccountRequest request) => await accounts.Register(request.Username, request.Password));
app.MapPost("/api/login", async (AccountRequest request) => await accounts.Login(request.Username, request.Password));
app.Map("/ws", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest) { context.Response.StatusCode = 400; return; }
    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    await hub.Handle(socket, context.RequestAborted);
});
app.Run();

sealed class ChatHub
{
    readonly ConcurrentDictionary<string, Room> rooms = new(StringComparer.OrdinalIgnoreCase);
    readonly JsonSerializerOptions json = new(JsonSerializerDefaults.Web);
    readonly AccountStore accounts;
    public ChatHub(AccountStore accounts) => this.accounts = accounts;

    public async Task Handle(WebSocket ws, CancellationToken ct)
    {
        Member? member = null; Room? room = null; string roomCode = "";
        try
        {
            var join = await Receive(ws, ct);
            if (join?.Type != "join") return;
            var accountName = accounts.Configured ? accounts.Validate(join.Token) : Clean(join.Name,24);
            if (accountName is null || accountName.Length==0) { await Send(ws, new { type = "error", message = "Your login expired. Please sign in again." }, ct); return; }
            roomCode = Clean(join.Room, 24).ToUpperInvariant();
            var name = accountName;
            if (roomCode.Length < 4 || name.Length < 1)
            { await Send(ws, new { type = "error", message = "Use a room code of at least 4 characters and enter a name." }, ct); return; }
            var existing = rooms.TryGetValue(roomCode, out room);
            await Send(ws, new { type = "keyRequired", create = !existing }, ct);
            var auth = await Receive(ws, ct);
            if (auth?.Type != "auth" || auth.Key is not { Length: >= 4 and <= 8 } || !auth.Key.All(char.IsDigit))
            { await Send(ws, new { type = "error", message = "The security PIN must contain 4 to 8 digits." }, ct); return; }
            var proposedKey = auth.Key;
            room = rooms.GetOrAdd(roomCode, _ => new Room(Hash(proposedKey)));
            member = new Member(Guid.NewGuid().ToString("N")[..8], name, ws);
            bool isOwner;
            lock (room.Gate)
            {
                if (!CryptographicOperations.FixedTimeEquals(room.KeyHash, Hash(proposedKey)))
                { Send(ws, new { type = "error", message = "Incorrect room key." }, ct).GetAwaiter().GetResult(); return; }
                isOwner = room.Members.IsEmpty;
                if (isOwner) room.OwnerId = member.Id;
                if (room.Members.Values.Any(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                    member = member with { Name = $"{name}-{Random.Shared.Next(10, 99)}" };
                room.Members[member.Id] = member;
            }
            await Send(ws, new { type = "welcome", id = member.Id, room = roomCode, name = member.Name, owner = isOwner }, ct);
            await Broadcast(room, new { type = "notice", message = $"{member.Name} joined the room.", timestamp = DateTimeOffset.UtcNow }, ct);
            await BroadcastMembers(room, ct);

            while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                var msg = await Receive(ws, ct); if (msg is null) break;
                if (msg.Type == "chat")
                {
                    var text = (msg.Message ?? "").Trim();
                    if (text.Length is > 0 and <= 1000)
                    {
                        var id = Guid.NewGuid().ToString("N")[..10]; room.Owners[id] = member.Id;
                        await Broadcast(room, new { type = "chat", id, senderId = member.Id, sender = member.Name, message = text, timestamp = DateTimeOffset.UtcNow }, ct);
                    }
                }
                else if (msg.Type == "image" && msg.Image is { Length: > 0 and <= 1_500_000 })
                {
                    var id = Guid.NewGuid().ToString("N")[..10]; room.Owners[id] = member.Id;
                    await Broadcast(room, new { type = "image", id, senderId = member.Id, sender = member.Name, image = msg.Image, timestamp = DateTimeOffset.UtcNow }, ct);
                }
                else if (msg.Type == "file" && msg.Data is { Length: > 0 and <= 7_000_000 } && CleanFileName(msg.FileName) is { Length: > 0 } fileName)
                {
                    var id = Guid.NewGuid().ToString("N")[..10]; room.Owners[id] = member.Id;
                    await Broadcast(room, new { type = "file", id, senderId = member.Id, sender = member.Name, fileName, data = msg.Data, timestamp = DateTimeOffset.UtcNow }, ct);
                }
                else if (msg.Type == "delete" && msg.Id is { Length: > 0 } id && room.Owners.TryGetValue(id, out var owner) && owner == member.Id)
                {
                    room.Owners.TryRemove(id, out _);
                    await Broadcast(room, new { type = "delete", id }, ct);
                }
                else if (msg.Type == "dump" && room.OwnerId == member.Id)
                {
                    room.Owners.Clear();
                    await Broadcast(room, new { type = "dump", by = member.Name }, ct);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        finally
        {
            if (room is not null && member is not null)
            {
                lock (room.Gate) room.Members.TryRemove(member.Id, out _);
                if (room.Members.IsEmpty) rooms.TryRemove(roomCode, out _);
                else
                {
                    await Broadcast(room, new { type = "notice", message = $"{member.Name} left the room.", timestamp = DateTimeOffset.UtcNow }, CancellationToken.None);
                    await BroadcastMembers(room, CancellationToken.None);
                }
            }
        }
    }

    async Task<Incoming?> Receive(WebSocket ws, CancellationToken ct)
    {
        var buffer = new byte[16_384]; using var data = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await ws.ReceiveAsync(buffer, ct);
            if (result.MessageType == WebSocketMessageType.Close) return null;
            data.Write(buffer, 0, result.Count);
            if (data.Length > 9_000_000) return null;
        } while (!result.EndOfMessage);
        try { return JsonSerializer.Deserialize<Incoming>(data.ToArray(), json); } catch { return null; }
    }
    async Task Send(WebSocket ws, object value, CancellationToken ct)
    {
        if (ws.State != WebSocketState.Open) return;
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, json));
        await ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
    }
    async Task Broadcast(Room room, object value, CancellationToken ct)
    {
        Member[] recipients; lock (room.Gate) recipients = room.Members.Values.ToArray();
        await Task.WhenAll(recipients.Select(m => Send(m.Socket, value, ct).ContinueWith(_ => { }, TaskScheduler.Default)));
    }
    Task BroadcastMembers(Room room, CancellationToken ct)
    {
        string[] names; lock (room.Gate) names = room.Members.Values.Select(m => m.Name).Order().ToArray();
        return Broadcast(room, new { type = "members", members = names }, ct);
    }
    static string Clean(string? text, int max) => new string((text ?? "").Where(c => char.IsLetterOrDigit(c) || c is ' ' or '-' or '_').Take(max).ToArray()).Trim();
    static string CleanFileName(string? value) => Path.GetFileName(value ?? "").Trim().Length > 120 ? Path.GetFileName(value ?? "").Trim()[..120] : Path.GetFileName(value ?? "").Trim();
    static byte[] Hash(string value) => SHA256.HashData(Encoding.UTF8.GetBytes(value));
}
sealed class Room(byte[] keyHash) { public byte[] KeyHash { get; } = keyHash; public string? OwnerId { get; set; } public object Gate { get; } = new(); public ConcurrentDictionary<string, Member> Members { get; } = new(); public ConcurrentDictionary<string,string> Owners { get; } = new(); }
sealed record Member(string Id, string Name, WebSocket Socket);
sealed class Incoming { public string? Type { get; set; } public string? Room { get; set; } public string? Name { get; set; } public string? Token { get; set; } public string? Key { get; set; } public string? Message { get; set; } public string? Image { get; set; } public string? Id { get; set; } public string? FileName { get; set; } public string? Data { get; set; } }
sealed record AccountRequest(string Username,string Password);
