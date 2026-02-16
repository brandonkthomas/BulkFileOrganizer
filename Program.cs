using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;

internal static class Program
{
    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// File Action
    /// </summary>
    private enum FileAction
    {
        Copy,
        Move
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// All options
    /// </summary>
    private sealed class Options
    {
        public string Extension { get; init; } = "";
        public string InputDirectory { get; init; } = "";
        public string OutputDirectory { get; init; } = "";
        public int MaxDepth { get; init; } = -1;           // -1 => infinite
        public int Workers { get; init; } = Math.Max(1, Environment.ProcessorCount / 2);
        public FileAction Action { get; init; } = FileAction.Copy;
        public int QueueCapacity { get; init; } = 10_000;  // bounded to avoid unbounded memory growth
        public int ProgressEvery { get; init; } = 10_000;
        public int TestFiles { get; init; } = 0;           // 0 => disabled
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Entry Point
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    /// <exception cref="DirectoryNotFoundException"></exception>
    public static int Main(string[] args)
    {
        try
        {
            //
            // 0: Parse arguments
            //
            var opt = ParseArgs(args);

            //
            // Ensure the input directory exists
            //
            if (!Directory.Exists(opt.InputDirectory))
                throw new DirectoryNotFoundException($"Input directory not found: {opt.InputDirectory}");

            //
            // Create the output directory if it doesn't exist
            //
            Directory.CreateDirectory(opt.OutputDirectory);

            //
            // Print our options
            //
            Console.WriteLine($"Extension: {opt.Extension}");
            Console.WriteLine($"Input:     {opt.InputDirectory}");
            Console.WriteLine($"Output:    {opt.OutputDirectory}");
            Console.WriteLine($"Action:    {opt.Action}");
            Console.WriteLine($"Depth:     {(opt.MaxDepth < 0 ? "infinite" : opt.MaxDepth.ToString(CultureInfo.InvariantCulture))}");
            Console.WriteLine($"Workers:   {opt.Workers}");
            Console.WriteLine($"TestFiles: {opt.TestFiles}");
            Console.WriteLine();

            //
            // Generate test files if requested
            //
            if (opt.TestFiles > 0)
            {
                GenerateTestFiles(opt.InputDirectory, opt.Extension, opt.TestFiles);
                Console.WriteLine();
            }

            //
            // Start the stopwatch
            //
            var sw = Stopwatch.StartNew();

            //
            // Initialize counters
            //
            long enumerated = 0;
            long processed = 0;
            long copied = 0;
            long moved = 0;
            long skipped = 0;
            long failed = 0;

            //
            // Create a cancellation token source
            //
            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
                Console.WriteLine("Cancellation requested...");
            };

            var queue = new BlockingCollection<(string SrcPath, string BucketName)>(boundedCapacity: opt.QueueCapacity);

            //
            // 1: Enumerate all matching files so we know the total count
            //
            var matchingFiles = new List<string>();

            try
            {
                foreach (var filePath in EnumerateFilesDepthLimited(opt.InputDirectory, opt.MaxDepth, cts.Token))
                {
                    if (cts.IsCancellationRequested) break;

                    if (!HasExtension(filePath, opt.Extension))
                        continue;

                    matchingFiles.Add(filePath);

                    Interlocked.Increment(ref enumerated);
                    var ecount = Interlocked.Read(ref enumerated);
                    if (ecount % opt.ProgressEvery == 0)
                    {
                        Console.WriteLine($"Enumerated: {ecount:n0} | Processed: {Interlocked.Read(ref processed):n0} | Failed: {Interlocked.Read(ref failed):n0}");
                    }
                }
            }
            catch (OperationCanceledException) { /* expected */ }
            catch (Exception ex)
            {
                Console.WriteLine($"Enumeration error: {ex}");
                cts.Cancel();
            }

            //
            // 2: Calculate disk usage and confirm processing
            //
            if (cts.IsCancellationRequested)
            {
                Console.WriteLine("Cancellation requested before processing.");
            }
            else if (matchingFiles.Count == 0)
            {
                Console.WriteLine("No files matched the specified extension.");
            }
            else
            {
                Console.WriteLine("Calculating disk usage...");

                long totalBytes = 0;
                foreach (var path in matchingFiles)
                {
                    try
                    {
                        totalBytes += new FileInfo(path).Length;
                    }
                    catch
                    {
                        // not a showstopper
                    }
                }

                var sizeText = FormatBytes(totalBytes);

                Console.WriteLine();
                Console.WriteLine($"About to {opt.Action} {matchingFiles.Count:n0} file(s) (~{sizeText}) to:");
                Console.WriteLine($"  {opt.OutputDirectory}");
                Console.Write("Continue? [y/N]: ");

                //
                // await user input
                //
                string? response = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(response) ||
                    !(response.Equals("y", StringComparison.OrdinalIgnoreCase) ||
                      response.Equals("yes", StringComparison.OrdinalIgnoreCase)))
                {
                    Console.WriteLine("Operation cancelled by user before processing.");
                    return 0;
                }

                //
                // 3: Decide how many buckets to create and how many files per bucket
                //
                var totalFiles = matchingFiles.Count;
                var bucketCount = (int)Math.Round(Math.Sqrt(totalFiles));
                if (bucketCount < 1) bucketCount = 1;
                if (bucketCount > totalFiles) bucketCount = totalFiles;
                if (bucketCount > 0x10000) bucketCount = 0x10000; // stay within 4-hex-digit space

                var basePerBucket = totalFiles / bucketCount;
                var remainder = totalFiles % bucketCount;

                //
                // Track successfully written destination paths for verification.
                //
                var successDestBySrc = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                //
                // 4: Create consumers to copy/move files
                //
                var consumers = new Task[opt.Workers];
                for (int i = 0; i < consumers.Length; i++)
                {
                    //
                    // 4.1: Create a consumer task
                    //
                    consumers[i] = Task.Run(() =>
                    {
                        //
                        // 4.2: Process items from the queue
                        //
                        foreach (var item in queue.GetConsumingEnumerable(cts.Token))
                        {
                            if (cts.IsCancellationRequested) break;

                            var srcPath = item.SrcPath;
                            var bucketName = item.BucketName;

                            try
                            {
                                //
                                // 4.2.1: Create the bucket directory
                                //
                                var bucketDir = Path.Combine(opt.OutputDirectory, bucketName);
                                Directory.CreateDirectory(bucketDir);

                                //
                                // 4.2.2: Get the file name and destination path
                                //
                                var fileName = Path.GetFileName(srcPath);
                                var destPath = Path.Combine(bucketDir, fileName);

                                //
                                // 4.2.3: Ensure the destination path is unique
                                //
                                destPath = EnsureUniquePath(destPath, srcPath);

                                if (opt.Action == FileAction.Copy)
                                {
                                    //
                                    // 4.2.4: Copy the file
                                    //
                                    File.Copy(srcPath, destPath, overwrite: false);
                                    Interlocked.Increment(ref copied);
                                }
                                else
                                {
                                    //
                                    // 4.2.5: Move the file
                                    //
                                    try
                                    {
                                        File.Move(srcPath, destPath);
                                    }
                                    catch (IOException)
                                    {
                                        File.Copy(srcPath, destPath, overwrite: false);
                                        File.Delete(srcPath);
                                    }
                                    Interlocked.Increment(ref moved);
                                }

                                //
                                // Record successful destination for verification.
                                //
                                successDestBySrc[srcPath] = destPath;

                                //
                                // 4.2.6: Increment the processed count
                                //
                                Interlocked.Increment(ref processed);

                                var pcount = Interlocked.Read(ref processed);
                                if (pcount % opt.ProgressEvery == 0)
                                {
                                    Console.WriteLine($"Processed: {pcount:n0} | Copied: {Interlocked.Read(ref copied):n0} | Moved: {Interlocked.Read(ref moved):n0} | Failed: {Interlocked.Read(ref failed):n0}");
                                }
                            }
                            catch (OperationCanceledException) { break; }
                            catch (UnauthorizedAccessException)
                            {
                                Interlocked.Increment(ref skipped);
                                Interlocked.Increment(ref processed);
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failed);
                                Interlocked.Increment(ref processed);
                                Console.WriteLine($"Failed: {srcPath}\n  {ex.GetType().Name}: {ex.Message}");
                            }
                        }
                    }, cts.Token);
                }

                //
                // 5: Assign files to buckets as evenly as possible and push into queue
                //
                try
                {
                    //
                    // 5.1: Assign files to buckets
                    //
                    int fileIndex = 0;
                    for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++)
                    {
                        //
                        // 5.1.1: Calculate the number of files for this bucket
                        //
                        int thisBucketCount = basePerBucket + (bucketIndex < remainder ? 1 : 0);
                        var bucketName = bucketIndex.ToString("x4", CultureInfo.InvariantCulture);

                        //
                        // 5.1.2: Add files to the queue
                        //
                        for (int j = 0; j < thisBucketCount; j++)
                        {
                            if (cts.IsCancellationRequested) break;

                            //
                            // Get the file path
                            //
                            var srcPath = matchingFiles[fileIndex++];
                            queue.Add((srcPath, bucketName), cts.Token);
                        }

                        if (cts.IsCancellationRequested)
                            break;
                    }
                }
                catch (OperationCanceledException) { /* expected */ }
                finally
                {
                    queue.CompleteAdding();
                }

                //
                // 6: Wait for all consumers to complete
                //
                Task.WaitAll(consumers);

                //
                // 7: Verification pass: ensure each successfully processed file exists in the output.
                //
                var missingFromOutput = new List<string>();
                foreach (var kvp in successDestBySrc)
                {
                    var destPath = kvp.Value;
                    if (!File.Exists(destPath))
                    {
                        missingFromOutput.Add(kvp.Key);
                    }
                }

                Console.WriteLine();
                Console.WriteLine($"Verified in output: {(successDestBySrc.Count - missingFromOutput.Count):n0} file(s).");
                if (missingFromOutput.Count > 0)
                {
                    Console.WriteLine($"Verification WARNING: {missingFromOutput.Count:n0} file(s) were expected but not found in the output directories.");
                    foreach (var src in missingFromOutput.Take(10))
                    {
                        Console.WriteLine($"  Missing output for: {src}");
                    }
                    if (missingFromOutput.Count > 10)
                    {
                        Console.WriteLine($"  ... and {(missingFromOutput.Count - 10):n0} more");
                    }
                }
            }

            //
            // 8: Stop the stopwatch and print the results... we're done (!!!)
            //
            sw.Stop();

            Console.WriteLine();
            Console.WriteLine("Done.");
            Console.WriteLine($"Enumerated: {enumerated:n0}");
            Console.WriteLine($"Processed:  {processed:n0}");
            Console.WriteLine($"Copied:     {copied:n0}");
            Console.WriteLine($"Moved:      {moved:n0}");
            Console.WriteLine($"Skipped:    {skipped:n0}");
            Console.WriteLine($"Failed:     {failed:n0}");
            Console.WriteLine($"Elapsed:    {sw.Elapsed}");

            return failed > 0 ? 2 : 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            PrintUsage();
            return 1;
        }
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Parse arguments
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    private static Options ParseArgs(string[] args)
    {
        // Minimal argument parser:
        // --ext .jpg
        // --input C:\in
        // --output D:\out
        // --action copy|move
        // --depth N
        // --workers N
        // --queue N
        // --progress N

        string? ext = null;
        string? input = null;
        string? output = null;
        string? action = null;
        int depth = -1;
        int workers = Math.Max(1, Environment.ProcessorCount / 2);
        int queue = 10_000;
        int progress = 10_000;
        int testFiles = 0;

        for (int i = 0; i < args.Length; i++)
        {
            var a = args[i];

            string Next()
            {
                if (i + 1 >= args.Length) throw new ArgumentException($"Missing value for {a}");
                return args[++i];
            }

            if (a.Equals("--ext", StringComparison.OrdinalIgnoreCase)) ext = Next();
            else if (a.Equals("--input", StringComparison.OrdinalIgnoreCase)) input = Next();
            else if (a.Equals("--output", StringComparison.OrdinalIgnoreCase)) output = Next();
            else if (a.Equals("--action", StringComparison.OrdinalIgnoreCase)) action = Next();
            else if (a.Equals("--depth", StringComparison.OrdinalIgnoreCase)) depth = int.Parse(Next(), CultureInfo.InvariantCulture);
            else if (a.Equals("--workers", StringComparison.OrdinalIgnoreCase)) workers = int.Parse(Next(), CultureInfo.InvariantCulture);
            else if (a.Equals("--queue", StringComparison.OrdinalIgnoreCase)) queue = int.Parse(Next(), CultureInfo.InvariantCulture);
            else if (a.Equals("--progress", StringComparison.OrdinalIgnoreCase)) progress = int.Parse(Next(), CultureInfo.InvariantCulture);
            else if (a.Equals("--test", StringComparison.OrdinalIgnoreCase)) testFiles = int.Parse(Next(), CultureInfo.InvariantCulture);
            else if (a.Equals("--help", StringComparison.OrdinalIgnoreCase) || a.Equals("-h", StringComparison.OrdinalIgnoreCase))
            {
                PrintUsage();
                Environment.Exit(0);
            }
            else
            {
                throw new ArgumentException($"Unknown argument: {a}");
            }
        }

        if (string.IsNullOrWhiteSpace(ext) ||
            string.IsNullOrWhiteSpace(input) ||
            string.IsNullOrWhiteSpace(output))
        {
            throw new ArgumentException("Missing required arguments.");
        }

        ext = NormalizeExtension(ext);

        var fileAction = FileAction.Copy;
        if (!string.IsNullOrWhiteSpace(action))
        {
            if (action.Equals("copy", StringComparison.OrdinalIgnoreCase)) fileAction = FileAction.Copy;
            else if (action.Equals("move", StringComparison.OrdinalIgnoreCase)) fileAction = FileAction.Move;
            else throw new ArgumentException("--action must be 'copy' or 'move'");
        }

        if (depth < -1) throw new ArgumentOutOfRangeException(nameof(depth), "--depth must be -1 (infinite) or >= 0");
        if (workers < 1) throw new ArgumentOutOfRangeException(nameof(workers), "--workers must be >= 1");
        if (queue < 1) throw new ArgumentOutOfRangeException(nameof(queue), "--queue must be >= 1");
        if (progress < 1) throw new ArgumentOutOfRangeException(nameof(progress), "--progress must be >= 1");
        if (testFiles < 0) throw new ArgumentOutOfRangeException(nameof(testFiles), "--test must be >= 0");

        return new Options
        {
            Extension = ext,
            InputDirectory = Path.GetFullPath(input),
            OutputDirectory = Path.GetFullPath(output),
            Action = fileAction,
            MaxDepth = depth,
            Workers = workers,
            QueueCapacity = queue,
            ProgressEvery = progress,
            TestFiles = testFiles
        };
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Print usage
    /// </summary>
    private static void PrintUsage()
    {
        Console.WriteLine(@"
Usage:
  SplitFiles.exe --ext <extension> --input <dir> --output <dir> [--action copy|move] [--depth N] [--workers N] [--queue N] [--progress N] [--test N]

Required:
  --ext      File extension to include (e.g. .jpg or jpg). Case-insensitive.
  --input    Root directory to search recursively.
  --output   Output root directory. Evenly sized bucket directories are created under this.

Optional:
  --action    copy (default) or move
  --depth     Max recursion depth. -1 (default) = infinite. 0 = only input directory.
  --workers   Number of parallel workers. Default = CPU/2
  --queue     Bounded queue capacity. Default = 10000
  --progress  Print progress every N processed/enumerated items. Default = 10000
  --test      Generate N empty files in the input directory (with the chosen extension) before processing.

Notes:
  - Files are evenly distributed into hex-named bucket directories (0000, 0001, ...) based on the total file count.
  - Name collisions are resolved by appending a short hash suffix.
");
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Normalize extension
    /// </summary>
    /// <param name="ext"></param>
    /// <returns></returns>
    private static string NormalizeExtension(string ext)
    {
        ext = ext.Trim();
        if (!ext.StartsWith(".", StringComparison.Ordinal)) ext = "." + ext;
        return ext.ToLowerInvariant();
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Check if file has extension
    /// </summary>
    /// <param name="filePath"></param>
    /// <param name="normalizedExt"></param>
    /// <returns></returns>
    private static bool HasExtension(string filePath, string normalizedExt)
    {
        // Avoid allocations; Path.GetExtension allocates.
        // This is a simple ends-with check on the normalized extension.
        return filePath.EndsWith(normalizedExt, StringComparison.OrdinalIgnoreCase);
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Format byte count into a human-readable string (KB, MB, GB, ...)
    /// </summary>
    /// <param name="bytes"></param>
    /// <returns></returns>
    private static string FormatBytes(long bytes)
    {
        string[] units = { "B", "KB", "MB", "GB", "TB", "PB" };
        double value = bytes;
        int unitIndex = 0;

        while (value >= 1024 && unitIndex < units.Length - 1)
        {
            value /= 1024;
            unitIndex++;
        }

        return $"{value:0.##} {units[unitIndex]}";
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Generate test files
    /// </summary>
    /// <param name="inputDir"></param>
    /// <param name="normalizedExt"></param>
    /// <param name="count"></param>
    private static void GenerateTestFiles(string inputDir, string normalizedExt, int count)
    {
        if (count <= 0)
            return;

        Directory.CreateDirectory(inputDir);

        Console.WriteLine($"Creating {count} empty test file(s) in input directory...");

        for (int i = 0; i < count; i++)
        {
            var fileName = $"bulk_test_{Guid.NewGuid():N}{normalizedExt}";
            var fullPath = Path.Combine(inputDir, fileName);

            // In the extremely unlikely case of a collision, try a few times.
            for (int attempt = 0; attempt < 10 && File.Exists(fullPath); attempt++)
            {
                fileName = $"bulk_test_{Guid.NewGuid():N}{normalizedExt}";
                fullPath = Path.Combine(inputDir, fileName);
            }

            using (File.Create(fullPath))
            {
                // Empty file, created and immediately closed.
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Enumerate files depth limited
    /// </summary>
    /// <param name="rootDir"></param>
    /// <param name="maxDepth"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    private static IEnumerable<string> EnumerateFilesDepthLimited(string rootDir, int maxDepth, CancellationToken ct)
    {
        // Iterative DFS to avoid recursion stack issues.
        var stack = new Stack<(string Dir, int Depth)>();
        stack.Push((rootDir, 0));

        while (stack.Count > 0)
        {
            ct.ThrowIfCancellationRequested();

            var (dir, depth) = stack.Pop();

            IEnumerable<string> entries;
            try
            {
                // Enumerate everything; we filter to files later.
                entries = Directory.EnumerateFileSystemEntries(dir);
            }
            catch (UnauthorizedAccessException)
            {
                continue;
            }
            catch (DirectoryNotFoundException)
            {
                continue;
            }

            foreach (var entry in entries)
            {
                ct.ThrowIfCancellationRequested();

                // Directory?
                if (IsDirectory(entry))
                {
                    if (maxDepth < 0 || depth < maxDepth)
                    {
                        stack.Push((entry, depth + 1));
                    }
                }
                else
                {
                    yield return entry;
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Check if path is a directory
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    private static bool IsDirectory(string path)
    {
        try
        {
            var attr = File.GetAttributes(path);
            return (attr & FileAttributes.Directory) != 0;
        }
        catch
        {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Ensure the destination path is unique
    /// </summary>
    /// <param name="destPath"></param>
    /// <param name="srcPath"></param>
    /// <returns></returns>
    private static string EnsureUniquePath(string destPath, string srcPath)
    {
        if (!File.Exists(destPath))
            return destPath;

        // Append an 8-hex suffix derived from the same hash to make collisions extremely unlikely.
        ulong hash = 14695981039346656037UL;
        for (int i = 0; i < srcPath.Length; i++)
        {
            hash ^= srcPath[i];
            hash *= 1099511628211UL;
        }

        var dir = Path.GetDirectoryName(destPath) ?? "";
        var fileName = Path.GetFileNameWithoutExtension(destPath);
        var ext = Path.GetExtension(destPath);

        string candidate;
        for (int attempt = 0; attempt < 1000; attempt++)
        {
            uint suffix = (uint)((hash >> (attempt % 32)) & 0xFFFFFFFF);
            candidate = Path.Combine(dir, $"{fileName}_{suffix:x8}{ext}");
            if (!File.Exists(candidate))
                return candidate;
        }

        // Worst case fallback (should be extremely rare)
        candidate = Path.Combine(dir, $"{fileName}_{Guid.NewGuid():N}{ext}");
        return candidate;
    }
}
