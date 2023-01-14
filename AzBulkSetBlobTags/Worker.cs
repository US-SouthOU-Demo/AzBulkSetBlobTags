using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AzBulkSetBlobTags
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IHostApplicationLifetime _hostApplicationLifetime;
        private TelemetryClient _telemetryClient;
        private Config _config;
        private readonly ConcurrentBag<Task> _todo;
        private readonly SemaphoreSlim _slim;
        private long _blobCount;
        private long _blobBytes;
        private bool _configValid = true;


        public Worker(ILogger<Worker> logger,
            IHostApplicationLifetime hostApplicationLifetime,
            TelemetryClient telemetryClient,
            Config config)
        {
            _logger = logger;
            _hostApplicationLifetime = hostApplicationLifetime;
            _telemetryClient = telemetryClient;
            _config = config;
            _todo = new ConcurrentBag<Task>();

            using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("Setup"))
            {
                _logger.LogInformation($"Run = {_config.Run}");
                op.Telemetry.Properties.Add("Run", _config.Run);

                _logger.LogInformation($"Container = {_config.Container}");
                op.Telemetry.Properties.Add("Container", _config.Container);

                var blobServiceClient = new BlobServiceClient(_config.StorageConnectionString);
                _logger.LogInformation($"StorageAccountName = {blobServiceClient.AccountName}");
                op.Telemetry.Properties.Add("StorageAccountName", blobServiceClient.AccountName);

                //Default the delimiter to a slash if not provided
                if (string.IsNullOrEmpty(_config.Delimiter))
                {
                    _config.Delimiter = "/";
                }
                _logger.LogInformation($"Delimiter = {_config.Delimiter}");
                op.Telemetry.Properties.Add("Delimiter", _config.Delimiter);

                //Set the starting point to the root if not provided
                if (string.IsNullOrEmpty(_config.Prefix))
                {
                    _config.Prefix = string.Empty;
                }
                //If starting point is provided, ensure that it has the delimiter at the end
                else if (!_config.Prefix.EndsWith(_config.Delimiter))
                {
                    _config.Prefix = _config.Prefix + _config.Delimiter;
                }
                _logger.LogInformation($"Prefix = {_config.Prefix}");
                op.Telemetry.Properties.Add("Prefix", _config.Prefix);

                //Set the default thread count if one was not set
                if (_config.ThreadCount < 1)
                {
                    _config.ThreadCount = Environment.ProcessorCount * 8;
                }
                _logger.LogInformation($"ThreadCount = {_config.ThreadCount}");
                op.Telemetry.Properties.Add("ThreadCount", _config.ThreadCount.ToString());

                //The Semaphore ensures how many scans can happen at the same time
                _slim = new SemaphoreSlim(_config.ThreadCount);
                
                _logger.LogInformation($"WhatIf = {_config.WhatIf}");
                op.Telemetry.Properties.Add("WhatIf", _config.WhatIf.ToString());

                if (string.IsNullOrEmpty(config.Container))
                {
                    _logger.LogError($"No Storage Container Name Provided.");
                    _configValid = false;
                }

            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.Register(() =>
            {
                _logger.LogInformation("Worker Cancelling");
            });

            try
            {
                if (_configValid)
                {
                    await DoWork(stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Operation Canceled");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled Exception");
            }
            finally
            {
                _logger.LogInformation("Flushing App Insights");
                _telemetryClient.Flush();
                Task.Delay(5000).Wait();

                _hostApplicationLifetime.StopApplication();
            }

        }

        private async Task DoWork(CancellationToken stoppingToken)
        {
            using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("Do Work"))
            {
                op.Telemetry.Properties.Add("Run", _config.Run);

                ProcessFolder(_config.Prefix, stoppingToken);

                //wait for enough to get the todo list so we don't exit before we started
                await Task.Delay(1000);

                // wait while there are any tasks that have not finished
                while (_todo.Any(x => !x.IsCompleted))
                {
                    LogStatus();
                    await Task.Delay(10000);
                }

                _logger.LogInformation("Done!");
                LogStatus();

                op.Telemetry.Metrics.Add("Blobs", _blobCount);
                op.Telemetry.Metrics.Add("Bytes", _blobBytes);
            }
        }

        /// <summary>
        /// Log information to the default logger
        /// </summary>
        private void LogStatus()
        {
            _logger.LogInformation($"Blobs Tagged: {_blobCount:N0} in {BytesToTiB(_blobBytes):N2} TiB");
        }

        /// <summary>
        /// convert bytes to TiBs
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        private double BytesToTiB(long bytes)
        {
            return bytes / Math.Pow(2, 40);
        }

        /// <summary>
        /// Process a Folder/Prefix of objects from your storage account
        /// </summary>
        /// <param name="prefix"></param>
        /// <param name="stoppingToken"></param>
        private void ProcessFolder(string prefix, CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Processing Prefix {prefix}");

            //Create a new task to process the folder
            _todo.Add(Task.Run(async () =>
            {
                await _slim.WaitAsync(stoppingToken);

                using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessPrefix"))
                {
                    op.Telemetry.Properties.Add("Run", _config.Run);
                    op.Telemetry.Properties.Add("Prefix", prefix);

                    //Get a client to connect to the blob container
                    var blobServiceClient = new BlobServiceClient(_config.StorageConnectionString);
                    var blobContainerClient = blobServiceClient.GetBlobContainerClient(_config.Container);
                    var uris = new Stack<Uri>();

                    await foreach (var item in blobContainerClient.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: _config.Delimiter, cancellationToken: stoppingToken))
                    {
                        //I found another folder, recurse
                        if (item.IsPrefix)
                        {
                            ProcessFolder(item.Prefix, stoppingToken);
                        }
                        //I found a block blob - set extension tag if NOT Archive
                        else if (item.IsBlob && BlobType.Block.Equals(item.Blob.Properties.BlobType))
                        {
                            InterlockedAdd(ref _blobCount, ref _blobBytes, item);

                            Dictionary<string, string> tags = new Dictionary<string, string>
                            {
                                { "ContentType", item.Blob.Properties.ContentType  },
                                { "Extension", item.Blob.Name.Split('.').Last() }

                            };

                            Azure.Response response = blobContainerClient.GetBlobClient(item.Blob.Name).SetTags(tags);
                            if (response.IsError) _logger.LogError(response.ReasonPhrase);
                        }

                    }

                }

                _slim.Release();

            }));

        }

        /// <summary>
        /// increment the counter
        /// </summary>
        /// <param name="count">blob count counter</param>
        /// <param name="bytes">blob bytes counter</param>
        /// <param name="bhi">blob hierarchy item</param>
        private void InterlockedAdd(ref long count, ref long bytes, BlobHierarchyItem bhi)
        {
            Interlocked.Add(ref count, 1);
            Interlocked.Add(ref bytes, bhi.Blob.Properties.ContentLength.GetValueOrDefault());
        }        
    }
}
