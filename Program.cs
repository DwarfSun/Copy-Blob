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
            string[] units = { "B", "KB", "MB", "GB", "TB" };
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
                Console.WriteLine("Usage: Copy-Blob --blob-url <url> [--local-path <path> --account-key <key>]");
                return 1;
            }

            string? blobUrl = null;
            string? localPath = null;
            string? accountKey = null;

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "--blob-url" && i + 1 < args.Length)
                    blobUrl = args[++i];
                else if (args[i] == "--local-path" && i + 1 < args.Length)
                    localPath = args[++i];
                else if (args[i] == "--account-key" && i + 1 < args.Length)
                    accountKey = args[++i];
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

            await DownloadBlobWithResume(blobUrl, localPath, accountKey);
            return 0;
        }

        static async Task DownloadBlobWithResume(string blobUrl, string localPath, string? accountKey)
        {
            BlobClient blobClient;
            if (!string.IsNullOrEmpty(accountKey))
            {
                // Use account key authentication
                var blobUri = new Uri(blobUrl);
                var accountName = blobUri.Host.Split('.')[0];
                var credential = new Azure.Storage.StorageSharedKeyCredential(accountName, accountKey);
                blobClient = new BlobClient(blobUri, credential);
            }
            else
            {
                // Use DefaultAzureCredential (MS Entra ID)
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

            var options = new Azure.Storage.Blobs.Models.BlobDownloadOptions
            {
                Range = new Azure.HttpRange(existingLength)
            };
            var downloadResponse = await blobClient.DownloadStreamingAsync(options);

            const int bufferSize = 81920; // 80 KB
            byte[] buffer = new byte[bufferSize];
            long totalRead = existingLength;
            var stream = downloadResponse.Value.Content;
            var startTime = DateTime.UtcNow;
            var lastReportTime = startTime;
            long lastReportBytes = totalRead;

            using (var fileStream = new FileStream(localPath, FileMode.Append, FileAccess.Write))
            {
                int read;
                while ((read = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    await fileStream.WriteAsync(buffer, 0, read);
                    totalRead += read;

                    var now = DateTime.UtcNow;
                    var elapsed = now - startTime;
                    var percent = (double)totalRead / blobLength * 100.0;
                    var speed = totalRead / 1024.0 / 1024.0 / elapsed.TotalSeconds; // MB/s
                    var eta = speed > 0 ? TimeSpan.FromSeconds((blobLength - totalRead) / 1024.0 / 1024.0 / speed) : TimeSpan.Zero;

                    string downloadedStr = FormatBytes(totalRead);
                    string totalStr = FormatBytes(blobLength);
                    string etaStr = eta.TotalDays >= 1
                        ? $"{(int)eta.TotalDays}d {eta.Hours:00}:{eta.Minutes:00}:{eta.Seconds:00}"
                        : eta.ToString(@"hh\:mm\:ss");

                    // Update every 0.5s or on completion
                    if ((now - lastReportTime).TotalSeconds > 0.5 || totalRead == blobLength)
                    {
                        Console.Write($"\rDownloaded: {downloadedStr}/{totalStr} | {percent:F2}% | Speed: {speed:F2} MB/s | Elapsed: {elapsed:hh\\:mm\\:ss} | ETA: {etaStr}   ");
                        lastReportTime = now;
                        lastReportBytes = totalRead;
                    }
                }
            }
            Console.WriteLine("\nDownload complete.");
        }
    }
}
