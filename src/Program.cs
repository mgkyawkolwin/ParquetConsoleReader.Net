using Parquet.Serialization;
using Microsoft.Data.Sqlite;
using ShellProgressBar;

class Program
{
    static async Task Main()
    {
        Console.Clear();
        Console.WriteLine("=== Parquet to SQLite Converter ===");
        
        // Get input folder path from user
        Console.WriteLine("Enter folder path containing Parquet files:");
        string folderPath = Console.ReadLine()?.Trim('"').Trim() ?? "";
        
        if (string.IsNullOrEmpty(folderPath) || !Directory.Exists(folderPath))
        {
            Console.WriteLine("❌ Folder not found or invalid path!");
            return;
        }

        // SQLite database path
        string sqlitePath = "parquet.sqlite";

        try
        {
            await ProcessParquetFolder(folderPath, sqlitePath);
            Console.WriteLine("\n✅ All files processed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Error: {ex.Message}");
        }

        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();
    }

    static async Task ProcessParquetFolder(string folderPath, string sqlitePath)
    {
        // Get all parquet files in the folder
        var parquetFiles = Directory.GetFiles(folderPath, "*.parquet");
        
        if (parquetFiles.Length == 0)
        {
            Console.WriteLine("❌ No .parquet files found in the specified folder!");
            return;
        }

        Console.WriteLine($"\n📁 Found {parquetFiles.Length} parquet files in folder");

        // Initialize SQLite database
        await InitializeDatabase(sqlitePath);

        // Process each file with overall progress
        using var mainProgressBar = new ProgressBar(parquetFiles.Length, "Processing files", new ProgressBarOptions
        {
            ForegroundColor = ConsoleColor.Cyan,
            BackgroundColor = ConsoleColor.DarkGray,
            ProgressCharacter = '─',
            ShowEstimatedDuration = true
        });

        int totalRowsProcessed = 0;
        int filesProcessed = 0;

        foreach (var filePath in parquetFiles)
        {
            try
            {
                mainProgressBar.Tick(filesProcessed, $"Processing: {Path.GetFileName(filePath)}");
                
                
                var rowsProcessed = await ProcessSingleParquetFile(filePath, sqlitePath);
                totalRowsProcessed += rowsProcessed;
                filesProcessed++;
                
                mainProgressBar.Tick(filesProcessed, $"✅ {Path.GetFileName(filePath)}: {rowsProcessed} rows");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ Failed to process {Path.GetFileName(filePath)}: {ex.Message}");
                filesProcessed++;
                mainProgressBar.Tick(filesProcessed, $"❌ {Path.GetFileName(filePath)}: Failed");
            }
        }

        Console.WriteLine($"\n📊 Final Summary: {totalRowsProcessed} total rows from {filesProcessed} files");
    }

    static async Task<int> ProcessSingleParquetFile(string filePath, string sqlitePath)
    {
        Console.WriteLine($"\n📖 Reading: {Path.GetFileName(filePath)}");
        
        // Read Parquet file
        var data = await ReadParquetWithProgress(filePath);
        
        // Save to SQLite
        var rowsInserted = await SaveToSqliteWithProgress(data, sqlitePath, Path.GetFileName(filePath));
        
        return rowsInserted;
    }

    static async Task InitializeDatabase(string sqlitePath)
    {
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
                source_file TEXT,
                created_date DATETIME DEFAULT CURRENT_TIMESTAMP
            )";

        using var createTableCommand = new SqliteCommand(createTableSql, connection);
        await createTableCommand.ExecuteNonQueryAsync();
    }

    static async Task<IList<ParquetData>> ReadParquetWithProgress(string filePath)
    {
        try
        {
            using var fileStream = File.OpenRead(filePath);
            var data = await ParquetSerializer.DeserializeAsync<ParquetData>(fileStream);
            return data;
        }
        catch (Exception ex)
        {
            throw new Exception($"Failed to read {Path.GetFileName(filePath)}: {ex.Message}", ex);
        }
    }

    static async Task<int> SaveToSqliteWithProgress(IList<ParquetData> data, string sqlitePath, string fileName)
    {
        if (data.Count == 0)
            return 0;

        using var connection = new SqliteConnection($"Data Source={sqlitePath}");
        await connection.OpenAsync();

        // Use transaction for better performance
        using var transaction = await connection.BeginTransactionAsync();
        
        var insertSql = @"
            INSERT INTO parquet_data 
            (language, speaker_id, prompt_id, prompt, segment_id, raw_text, iso_639_3, glottocode, iso_15924, source_file)
            VALUES 
            (@language, @speaker_id, @prompt_id, @prompt, @segment_id, @raw_text, @iso_639_3, @glottocode, @iso_15924, @source_file)";

        using var insertCommand = new SqliteCommand(insertSql, connection);
        insertCommand.Transaction = (SqliteTransaction)transaction;
        
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
            new SqliteParameter("@iso_15924", SqliteType.Text),
            new SqliteParameter("@source_file", SqliteType.Text)
        });

        int successCount = 0;
        int errorCount = 0;

        for (int i = 0; i < data.Count; i++)
        {
            try
            {
                var row = data[i];
                
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
                insertCommand.Parameters["@source_file"].Value = fileName;

                await insertCommand.ExecuteNonQueryAsync();
                successCount++;
            }
            catch (Exception ex)
            {
                errorCount++;
            }
        }

        await transaction.CommitAsync();
        
        Console.WriteLine($"📊 {fileName}: {successCount} successful, {errorCount} failed");
        return successCount;
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