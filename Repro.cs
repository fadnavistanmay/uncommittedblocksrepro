// filepath: c:\Users\tafadnav\source\repos\uncommittedblocks\Repro.cs
using System;
using System.IO;
using System.Threading.Tasks;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs.Models;
using Azure;

// Traditional class structure instead of top-level statements
public class Repro
{
    // Main entry point
    public static void Main(string[] args)
    {
        RunBlobTest2().GetAwaiter().GetResult();
    }

    private static async Task RunBlobTest2()
    {
        Console.WriteLine("Starting uncommitted blocks test with parallel block staging for a single blob...");

        // SAS URL for the blob container
        string sasUrl = "https://tafadnavblobdomain0.blob.core.windows.net/b-08e066a00563468facdd3e5f0f0b3def?sp=racwdl&st=2025-05-07T21:47:41Z&se=2025-05-08T05:47:41Z&skoid=278892fa-ea67-4a0d-8af4-b20d8a6f4a26&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2025-05-07T21:47:41Z&ske=2025-05-08T05:47:41Z&sks=b&skv=2024-11-04&spr=https&sv=2024-11-04&sr=c&sig=oZpPz5ebVRk1LcrI9nTt6f2qRVw441ZXu9aWQfMLckA%3D";

        // Declare variables at method scope so they are accessible in all try/catch blocks
        BlobContainerClient containerClient = null;
        string blobName = null;
        BlockBlobClient blobClient = null;

        // We'll use the same block ID for all blocks
        string blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("sameBlockId"));
        Console.WriteLine($"Using block ID: {blockId}");

        // Create a list with a single block ID for normal commits
        List<string> blockIds = new List<string>();
        blockIds.Add(blockId);

        // Create a large array of 101K block IDs all with the same value
        int totalBlocks = 101000;
        List<string> commitBlockIds = new List<string>(totalBlocks);
        for (int i = 0; i < totalBlocks; i++)
        {
            commitBlockIds.Add(blockId);
        }
        Console.WriteLine($"Created commitBlockIds with {commitBlockIds.Count} identical block IDs");

        try
        {
            // Create a BlobContainerClient using the SAS URL
            containerClient = new BlobContainerClient(new Uri(sasUrl));

            // Generate a random blob name
            blobName = $"testblob-{Guid.NewGuid()}.dat";
            Console.WriteLine($"Using blob name: {blobName}");

            // Get a reference to a blob
            blobClient = containerClient.GetBlockBlobClient(blobName);

            // Create some random data for the block
            byte[] blockData = new byte[1024]; // 1KB block
            Random rand = new Random();
            rand.NextBytes(blockData);

            // Configuration for parallel processing
            int maxConcurrentTasks = 50; // Maximum number of concurrent operations
            int batchSize = 5000; // Process blocks in batches
            int totalBatches = (int)Math.Ceiling(totalBlocks / (double)batchSize);

            Console.WriteLine($"Staging {totalBlocks} blocks with the same block ID in parallel");
            Console.WriteLine($"Using max concurrency: {maxConcurrentTasks}, batch size: {batchSize}");

            // Create semaphore to limit concurrency
            using var semaphore = new SemaphoreSlim(maxConcurrentTasks);
            int completedBlocks = 0;

            for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++)
            {
                int startIdx = batchIndex * batchSize;
                int currentBatchSize = Math.Min(batchSize, totalBlocks - startIdx);

                Console.WriteLine($"Processing batch {batchIndex + 1} of {totalBatches}: blocks {startIdx} to {startIdx + currentBatchSize - 1}");

                // Create a list of tasks for this batch
                var batchTasks = new List<Task>();

                // Start a task for each block in this batch
                for (int i = 0; i < currentBatchSize; i++)
                {
                    batchTasks.Add(Task.Run(async () =>
                    {
                        // Wait for a slot in the semaphore
                        await semaphore.WaitAsync();

                        try
                        {
                            // Create a new memory stream for each task to avoid sharing stream position
                            var blockDataCopy = new byte[blockData.Length];
                            Array.Copy(blockData, blockDataCopy, blockData.Length);

                            // Stage the block
                            await blobClient.StageBlockAsync(blockId, new MemoryStream(blockDataCopy));

                            // Increment the completed blocks count
                            Interlocked.Increment(ref completedBlocks);

                            // Log progress periodically (every 1000 blocks)
                            if (completedBlocks % 1000 == 0)
                            {
                                Console.WriteLine($"Staged {completedBlocks} blocks so far");
                            }
                        }
                        finally
                        {
                            // Release the semaphore slot
                            semaphore.Release();
                        }
                    }));
                }

                // Wait for all tasks in this batch to complete
                await Task.WhenAll(batchTasks);
                Console.WriteLine($"Completed batch {batchIndex + 1}: total blocks staged so far: {completedBlocks}");
            }

            // THIS CODE WON'T BE REACHED DUE TO THE BLOCK COUNT EXCEEDS LIMIT ERROR
            Console.WriteLine($"Finished staging {totalBlocks} blocks with the same block ID.");

            // Try to commit the blocks
            Console.WriteLine("Attempting to commit the block list with all 101K block IDs...");
            try
            {
                // Try to commit with all the block IDs (this should fail with BlockCountExceedsLimit)
                await blobClient.CommitBlockListAsync(commitBlockIds);
                Console.WriteLine("Successfully committed the block list with 101K block IDs!");
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == BlobErrorCode.BlockCountExceedsLimit)
            {
                Console.WriteLine($"Block count exceeds limit as expected: {ex.Message}");

                // Try committing with just one block ID
                Console.WriteLine("Now trying to commit with just one block ID...");
                //try {
                    await blobClient.CommitBlockListAsync(commitBlockIds);
                    Console.WriteLine("Successfully committed the block list with a single block ID!");
                //}


                // Delete the blob to clean up uncommitted blocks
                Console.WriteLine("Deleting blob to clean up uncommitted blocks...");
                await blobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);

                // Verify that the blob is deleted and no uncommitted blocks remain
                bool exists = await blobClient.ExistsAsync();
                Console.WriteLine($"Blob exists after deletion attempt: {exists}");

                if (exists)
                {
                    Console.WriteLine("Attempting to abort uncommitted blocks by deleting blob again...");
                    // Try a more forceful delete
                    await blobClient.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots);

                    // Check again
                    exists = await blobClient.ExistsAsync();
                    Console.WriteLine($"Blob exists after second deletion attempt: {exists}");
                }

                if (!exists)
                {
                    Console.WriteLine("Blob successfully deleted, all uncommitted blocks cleaned up");
                }
                else
                {
                    Console.WriteLine("Warning: Blob could not be fully deleted");
                }
            }
        }
        catch (RequestFailedException ex)
        {
            Console.WriteLine($"Azure Storage error: {ex.Status}, {ex.ErrorCode}, {ex.Message}");

            if (ex.ErrorCode == BlobErrorCode.BlockCountExceedsLimit)
            {
                // This is a backup catch in case the inner try-catch doesn't handle it
                Console.WriteLine("Attempting to delete blob after error...");


                try
                {
                    // Try to commit with all the block IDs (this should fail with BlockCountExceedsLimit)
                    await blobClient!.CommitBlockListAsync(commitBlockIds);
                    Console.WriteLine("Successfully committed the block list with 101K block IDs!");
                }
                catch (RequestFailedException ex2) when (ex2.ErrorCode == BlobErrorCode.BlockListTooLong)
                {
                    Console.WriteLine($"Block count exceeds limit as expected: {ex2.Message}"); // This is the error we expect.
                }
                catch (Exception ex2)
                {
                    Console.WriteLine($"Unexpected error during block commit: {ex2.Message}");
                }
            }

        Console.WriteLine("Test completed. Press any key to exit.");
        Console.ReadKey();
    }
    }
}
