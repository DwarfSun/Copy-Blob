using System.CommandLine.Invocation;
using System.CommandLine;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Identity;
using System.IO;
using System.Threading.Tasks;

namespace Copy_Blob
{
    class Program
    {
        // Helper to format bytes in largest unit
        static string FormatBytes(double bytes)
        {
            string[] units = ["B", "KB", "MB", "GB", "TB"];
            int unitIndex = 0;
            while (unitIndex < units.Length - 1 && bytes >= 1024)
            {
                bytes /= 1024.0;
                unitIndex++;
            }
            return $"{bytes:F2} {units[unitIndex]}";
        }
        static async Task<int> Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: Copy-Blob --blob-url <url> [--threads <num|max> --local-path <path> --account-key <key>]");
                return 1;
            }

            string? blobUrl = null;
            string? localPath = null;
            string? accountKey = null;
            int threads = 1;
            bool autoThreads = false;

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "--blob-url" && i + 1 < args.Length)
                    blobUrl = args[++i];
                else if (args[i] == "--local-path" && i + 1 < args.Length)
                    localPath = args[++i];
                else if (args[i] == "--account-key" && i + 1 < args.Length)
                    accountKey = args[++i];
                else if (args[i] == "--threads" && i + 1 < args.Length)
                {
                    var val = args[++i];
                    if (val.Equals("max", StringComparison.CurrentCultureIgnoreCase))
                    {
                        autoThreads = true;
                    }
                    else if (int.TryParse(val, out int t) && t > 0)
                    {
                        threads = t;
                    }
                }
            }

            if (autoThreads)
            {
                // Use processor count, but cap at 8 for safety
                threads = Math.Min(Environment.ProcessorCount, 8);
            }

            if  (string.IsNullOrEmpty(localPath)) localPath = Directory.GetCurrentDirectory();

            if (string.IsNullOrEmpty(blobUrl))
            {
                Console.WriteLine("--blob-url is required.");
                return 1;
            }

            // If localPath is a directory, use the filename from the blob URL
            if (Directory.Exists(localPath))
            {
                try
                {
                    var blobUri = new Uri(blobUrl);
                    var fileName = Path.GetFileName(blobUri.LocalPath);
                    if (string.IsNullOrEmpty(fileName))
                    {
                        Console.WriteLine($"Error: Could not determine file name from blob URL '{blobUrl}'.");
                        return 1;
                    }
                    localPath = Path.Combine(localPath, fileName);
                    Console.WriteLine($"Info: Saving to '{localPath}'");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                    return 1;
                }
            }

            await DownloadBlobWithResume(blobUrl, localPath, accountKey, threads);
            return 0;
        }

        static async Task DownloadBlobWithResume(string blobUrl, string localPath, string? accountKey, int threads)
        {
            BlobClient blobClient;
            if (!string.IsNullOrEmpty(accountKey))
            {
                var blobUri = new Uri(blobUrl);
                var accountName = blobUri.Host.Split('.')[0];
                var credential = new Azure.Storage.StorageSharedKeyCredential(accountName, accountKey);
                blobClient = new BlobClient(blobUri, credential);
            }
            else
            {
                var blobUri = new Uri(blobUrl);
                var credential = new DefaultAzureCredential();
                blobClient = new BlobClient(blobUri, credential);
            }

            long existingLength = 0;
            if (File.Exists(localPath))
            {
                existingLength = new FileInfo(localPath).Length;
            }

            var properties = await blobClient.GetPropertiesAsync();
            long blobLength = properties.Value.ContentLength;

            if (existingLength >= blobLength)
            {
                Console.WriteLine("File already fully downloaded.");
                return;
            }

            const int chunkSize = 4 * 1024 * 1024; // 4 MB per chunk
            int totalChunks = (int)Math.Ceiling((double)blobLength / chunkSize);

            // Track which chunks are already downloaded
            bool[] chunkDone = new bool[totalChunks];
            long totalRead = existingLength;
            if (existingLength > 0)
            {
                // Mark completed chunks
                for (int i = 0; i < totalChunks; i++)
                {
                    long chunkStart = i * chunkSize;
                    long chunkEnd = Math.Min(chunkStart + chunkSize, blobLength);
                    if (existingLength >= chunkEnd)
                        chunkDone[i] = true;
                }
            }

            var progress = new System.Collections.Concurrent.ConcurrentDictionary<int, long>();
            var startTime = DateTime.UtcNow;
            var lastReportTime = startTime;

            var tasks = new List<Task>();
            var throttler = new System.Threading.SemaphoreSlim(threads);

            // Status update loop (runs independently)
            var statusTask = Task.Run(async () =>
            {
                while (true)
                {
                    long downloaded = existingLength + progress.Values.Sum();
                    var now = DateTime.UtcNow;
                    var elapsed = now - startTime;
                    var percent = (double)downloaded / blobLength * 100.0;
                    var speed = (downloaded - existingLength) / 1024.0 / 1024.0 / elapsed.TotalSeconds; // MB/s
                    var eta = speed > 0 ? TimeSpan.FromSeconds((blobLength - downloaded) / 1024.0 / 1024.0 / speed) : TimeSpan.Zero;

                    string downloadedStr = FormatBytes(downloaded);
                    string totalStr = FormatBytes(blobLength);
                    string etaStr = eta.TotalDays >= 1
                        ? $"{(int)eta.TotalDays}d {eta.Hours:00}:{eta.Minutes:00}:{eta.Seconds:00}"
                        : eta.ToString(@"hh\:mm\:ss");

                    Console.Write($"\rDownloaded: {downloadedStr}/{totalStr} | {percent:F2}% | Speed: {speed:F2} MB/s | Threads: {threads} | Elapsed: {elapsed:hh\\:mm\\:ss} | ETA: {etaStr}   ");

                    if (downloaded >= blobLength)
                        break;

                    await Task.Delay(500);
                }
            });

            // Download chunks in parallel
            for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++)
            {
                if (chunkDone[chunkIndex]) continue;
                await throttler.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        long offset = chunkIndex * chunkSize;
                        long length = Math.Min(chunkSize, blobLength - offset);
                        var options = new Azure.Storage.Blobs.Models.BlobDownloadOptions
                        {
                            Range = new Azure.HttpRange(offset, length)
                        };
                        var downloadResponse = await blobClient.DownloadStreamingAsync(options);
                        using (var chunkStream = downloadResponse.Value.Content)
                        using (var fs = new FileStream(localPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Write))
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            byte[] buffer = new byte[81920];
                            int read;
                            long chunkRead = 0;
                            while ((read = await chunkStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                            {
                                await fs.WriteAsync(buffer, 0, read);
                                chunkRead += read;
                                progress[chunkIndex] = chunkRead;
                            }
                        }
                    }
                    finally
                    {
                        throttler.Release();
                    }
                }));
            }

            await Task.WhenAll(tasks);
            await statusTask;
            Console.WriteLine("\nDownload complete.");
        }
    }
}
