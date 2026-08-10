using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Npgsql;

sealed class AccountStore
{
    readonly string? connectionString;
    readonly ConcurrentDictionary<string,(string Name,DateTime Expires)> sessions=new();
    public bool Configured=>connectionString is not null;
    public AccountStore(string? url){if(!string.IsNullOrWhiteSpace(url))connectionString=ConvertUrl(url);}
    public async Task Initialize()
    {
        if(!Configured)return;await using var db=new NpgsqlConnection(connectionString);await db.OpenAsync();
        await using var cmd=new NpgsqlCommand("CREATE TABLE IF NOT EXISTS accounts (id BIGSERIAL PRIMARY KEY, username VARCHAR(20) UNIQUE NOT NULL, salt BYTEA NOT NULL, password_hash BYTEA NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",db);await cmd.ExecuteNonQueryAsync();
    }
    public async Task<IResult> Register(string? username,string? password)
    {
        if(!Configured)return Results.Problem("Accounts database is not configured.",statusCode:503);var name=(username??"").Trim();
        if(!Regex.IsMatch(name,"^[A-Za-z0-9_]{3,20}$"))return Results.BadRequest(new{message="Username must be 3–20 letters, numbers, or underscores."});
        if(password is null||password.Length is <8 or >128)return Results.BadRequest(new{message="Password must be 8–128 characters."});
        var salt=RandomNumberGenerator.GetBytes(16);var hash=Hash(password,salt);
        try{await using var db=new NpgsqlConnection(connectionString);await db.OpenAsync();await using var cmd=new NpgsqlCommand("INSERT INTO accounts(username,salt,password_hash) VALUES($1,$2,$3)",db);cmd.Parameters.AddWithValue(name);cmd.Parameters.AddWithValue(salt);cmd.Parameters.AddWithValue(hash);await cmd.ExecuteNonQueryAsync();return Success(name);}catch(PostgresException e)when(e.SqlState=="23505"){return Results.Conflict(new{message="That username is already taken."});}
    }
    public async Task<IResult> Login(string? username,string? password)
    {
        if(!Configured)return Results.Problem("Accounts database is not configured.",statusCode:503);var name=(username??"").Trim();
        await using var db=new NpgsqlConnection(connectionString);await db.OpenAsync();await using var cmd=new NpgsqlCommand("SELECT username,salt,password_hash FROM accounts WHERE LOWER(username)=LOWER($1)",db);cmd.Parameters.AddWithValue(name);await using var row=await cmd.ExecuteReaderAsync();
        if(!await row.ReadAsync()||password is null||!CryptographicOperations.FixedTimeEquals((byte[])row[2],Hash(password,(byte[])row[1])))return Results.Json(new{message="Incorrect username or password."},statusCode:401);return Success(row.GetString(0));
    }
    IResult Success(string name){var token=Convert.ToHexString(RandomNumberGenerator.GetBytes(32)).ToLowerInvariant();sessions[token]=(name,DateTime.UtcNow.AddDays(7));return Results.Ok(new{token,username=name});}
    public string? Validate(string? token){if(token is null||!sessions.TryGetValue(token,out var session)||session.Expires<DateTime.UtcNow){if(token is not null)sessions.TryRemove(token,out _);return null;}return session.Name;}
    static byte[] Hash(string password,byte[] salt)=>Rfc2898DeriveBytes.Pbkdf2(password,salt,210_000,HashAlgorithmName.SHA256,32);
    static string ConvertUrl(string value){var u=new Uri(value);var user=u.UserInfo.Split(':',2);return new NpgsqlConnectionStringBuilder{Host=u.Host,Port=u.Port>0?u.Port:5432,Username=Uri.UnescapeDataString(user[0]),Password=user.Length>1?Uri.UnescapeDataString(user[1]):"",Database=u.AbsolutePath.Trim('/'),SslMode=SslMode.Require}.ConnectionString;}
}
