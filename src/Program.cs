using Parquet;
using Parquet.Serialization;
using Microsoft.Data.Sqlite;
using ShellProgressBar;
using System.Data;

class Program
{
    static async Task Main()
    {
        Console.WriteLine("=== Parquet to SQLite Converter ===");
        
        // Get input file path from user
        Console.WriteLine("Enter Parquet file path:");
        string filePath = Console.ReadLine()?.Trim('"').Trim() ?? "";
        
        if (string.IsNullOrEmpty(filePath) || !File.Exists(filePath))
        {
            Console.WriteLine("❌ File not found or invalid path!");
            return;
        }

        // SQLite database path
        string sqlitePath = "parquet.sqlite";

        try
        {
            await ProcessParquetFile(filePath, sqlitePath);
            Console.WriteLine("\n✅ Conversion completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Error: {ex.Message}");
        }

        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();
    }

    static async Task ProcessParquetFile(string parquetPath, string sqlitePath)
    {
        // Read Parquet file
        Console.WriteLine($"\n📖 Reading Parquet file: {Path.GetFileName(parquetPath)}");
        var data = await ReadParquetWithProgress(parquetPath);
        
        // Save to SQLite
        Console.WriteLine($"\n💾 Saving to SQLite: {sqlitePath}");
        await SaveToSqliteWithProgress(data, sqlitePath);
    }

    static async Task<IList<ParquetData>> ReadParquetWithProgress(string filePath)
    {
        using var progressBar = new ProgressBar(100, "Reading Parquet file", new ProgressBarOptions
        {
            ForegroundColor = ConsoleColor.Green,
            BackgroundColor = ConsoleColor.DarkGray,
            ProgressCharacter = '─',
            ShowEstimatedDuration = true
        });

        try
        {
            // Simulate progress for reading (since we can't get actual progress from DeserializeAsync)
            for (int i = 0; i <= 100; i += 10)
            {
                await Task.Delay(50);
                progressBar.Tick(i, $"Reading file... {i}%");
            }

            var fileStream = File.OpenRead(filePath);
            var data = await ParquetSerializer.DeserializeAsync<ParquetData>(fileStream);
            
            progressBar.Tick(100, $"✅ Read {data.Count} rows");
            return data;
        }
        catch (Exception ex)
        {
            progressBar.Tick(100, "❌ Error reading file");
            throw new Exception($"Failed to read Parquet file: {ex.Message}", ex);
        }
    }

    static async Task SaveToSqliteWithProgress(IList<ParquetData> data, string sqlitePath)
    {
        using var progressBar = new ProgressBar(data.Count, "Saving to SQLite", new ProgressBarOptions
        {
            ForegroundColor = ConsoleColor.Blue,
            BackgroundColor = ConsoleColor.DarkGray,
            ProgressCharacter = '─',
            ShowEstimatedDuration = true
        });

        using var connection = new SqliteConnection($"Data Source={sqlitePath}");
        await connection.OpenAsync();

        // Create table if it doesn't exist
        var createTableSql = @"
            CREATE TABLE IF NOT EXISTS parquet_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                language TEXT,
                speaker_id TEXT,
                prompt_id TEXT,
                prompt TEXT,
                segment_id TEXT,
                raw_text TEXT,
                iso_639_3 TEXT,
                glottocode TEXT,
                iso_15924 TEXT,
                created_date DATETIME DEFAULT CURRENT_TIMESTAMP
            )";

        using var createTableCommand = new SqliteCommand(createTableSql, connection);
        await createTableCommand.ExecuteNonQueryAsync();

        // Use transaction for better performance
        using var transaction = await connection.BeginTransactionAsync();
        
        var insertSql = @"
            INSERT INTO parquet_data 
            (language, speaker_id, prompt_id, prompt, segment_id, raw_text, iso_639_3, glottocode, iso_15924)
            VALUES 
            (@language, @speaker_id, @prompt_id, @prompt, @segment_id, @raw_text, @iso_639_3, @glottocode, @iso_15924)";

        using var insertCommand = new SqliteCommand(insertSql, connection);
        insertCommand.Transaction = (SqliteTransaction) transaction;
        
        // Add parameters
        insertCommand.Parameters.AddRange(new[]
        {
            new SqliteParameter("@language", SqliteType.Text),
            new SqliteParameter("@speaker_id", SqliteType.Text),
            new SqliteParameter("@prompt_id", SqliteType.Text),
            new SqliteParameter("@prompt", SqliteType.Text),
            new SqliteParameter("@segment_id", SqliteType.Text),
            new SqliteParameter("@raw_text", SqliteType.Text),
            new SqliteParameter("@iso_639_3", SqliteType.Text),
            new SqliteParameter("@glottocode", SqliteType.Text),
            new SqliteParameter("@iso_15924", SqliteType.Text)
        });

        int successCount = 0;
        int errorCount = 0;
        int i = 0;

        foreach(ParquetData row in data)
        {
            try
            {

                
                // Set parameter values
                insertCommand.Parameters["@language"].Value = row.language ?? (object)DBNull.Value;
                insertCommand.Parameters["@speaker_id"].Value = row.speaker_id ?? (object)DBNull.Value;
                insertCommand.Parameters["@prompt_id"].Value = row.prompt_id ?? (object)DBNull.Value;
                insertCommand.Parameters["@prompt"].Value = row.prompt ?? (object)DBNull.Value;
                insertCommand.Parameters["@segment_id"].Value = row.segment_id ?? (object)DBNull.Value;
                insertCommand.Parameters["@raw_text"].Value = row.raw_text ?? (object)DBNull.Value;
                insertCommand.Parameters["@iso_639_3"].Value = row.iso_639_3 ?? (object)DBNull.Value;
                insertCommand.Parameters["@glottocode"].Value = row.glottocode ?? (object)DBNull.Value;
                insertCommand.Parameters["@iso_15924"].Value = row.iso_159_24 ?? (object)DBNull.Value;

                await insertCommand.ExecuteNonQueryAsync();
                successCount++;
                i++;
            }
            catch (Exception ex)
            {
                errorCount++;
                // You might want to log this error in a real application
            }

            progressBar.Tick($"Inserted: {i + 1}/{data.Count} | ✅: {successCount} | ❌: {errorCount}");
        }

        await transaction.CommitAsync();
        
        Console.WriteLine($"\n📊 Summary: {successCount} successful, {errorCount} failed");
    }
}

public class ParquetData
{
    public string? language { get; set; }
    public string? speaker_id { get; set; }
    public string? prompt_id { get; set; }
    public string? prompt { get; set; }
    public string? segment_id { get; set; }
    public string? raw_text { get; set; }
    public string? iso_639_3 { get; set; }
    public string? glottocode { get; set; }
    public string? iso_159_24 { get; set; }
}