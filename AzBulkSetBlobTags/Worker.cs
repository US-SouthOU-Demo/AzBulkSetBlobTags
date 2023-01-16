using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Azure.CognitiveServices.Vision.ComputerVision;
using Microsoft.Azure.CognitiveServices.Vision.ComputerVision.Models;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
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
        private long _imageTooBigToProcessCount;
        private long _imageProcessedCount;
        private bool _configValid = true;
        private string storedPolicyName = string.Empty;


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
                op.Telemetry.Metrics.Add("Blobs Too Big To Process", _imageTooBigToProcessCount);
                op.Telemetry.Metrics.Add("Images Processed", _imageProcessedCount);
            }
        }

        /// <summary>
        /// Log information to the default logger
        /// </summary>
        private void LogStatus()
        {
            _logger.LogInformation($"Blobs Tagged: {_blobCount:N0}; Images Processed: {_imageProcessedCount:N0}; images skipped processing: {_imageTooBigToProcessCount:N0}");
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
                    
                    // Get a client to create Thumbnails with
                    var webContainerClient = blobServiceClient.GetBlobContainerClient("$web");
                    webContainerClient.CreateIfNotExists();

                    await foreach (var item in blobContainerClient.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: _config.Delimiter, cancellationToken: stoppingToken))
                    {
                        //I found another folder, recurse
                        if (item.IsPrefix)
                        {
                            ProcessFolder(item.Prefix, stoppingToken);
                        }
                        //I found a block blob - set tags
                        else if (item.IsBlob && BlobType.Block.Equals(item.Blob.Properties.BlobType))
                        {
                            Interlocked.Add(ref _blobCount, 1);
                            IDictionary<string, string> tags = new Dictionary<string, string>();

                            BlobClient blobClient = blobContainerClient.GetBlobClient(item.Blob.Name);
                            
                            // Get any tags previously set
                            GetBlobTagResult getBlobTagResult = blobClient.GetTags();
                            if (getBlobTagResult.Tags != null && getBlobTagResult.Tags.Count > 1)
                                tags = getBlobTagResult.Tags;

                            // Set some tags. We get 10 key-value tags per Blob, up to 256 characters per tag value
                            // https://learn.microsoft.com/en-us/azure/storage/blobs/storage-manage-find-blobs?tabs=azure-portal#setting-blob-index-tags

                            if(!tags.ContainsKey("ContentType"))
                                tags["ContentType"] = item.Blob.Properties.ContentType;
                            
                            if(!tags.ContainsKey("Extension"))
                                tags["Extension"] = item.Blob.Name.Split('.').Last().ToLower();
                            else if(tags["Extension"] != item.Blob.Name.Split('.').Last().ToLower())
                                tags["Extension"] = item.Blob.Name.Split('.').Last().ToLower();
                            if(!tags.ContainsKey("MD5"))
                                tags["MD5"] = System.Convert.ToBase64String(item.Blob.Properties.ContentHash);

                            // Use ComputerVision to describe images, if we have been passed in the key to an existing service and it's endpoint (e.g. https://contoso.cognitiveservices.azure.com)
                            if(item.Blob.Properties.AccessTier != AccessTier.Archive && item.Blob.Properties.ContentType == "image/jpeg" && !tags.ContainsKey("description") && !string.IsNullOrEmpty(_config.ComputerVisionKey) && !string.IsNullOrEmpty(_config.ComputerVisionEndpoint) {

                                if(item.Blob.Properties.ContentLength > 4000000)
                                    // _logger.LogWarning(string.Format("Image '{0}' is size {1} bytes and therefore {2} bytes too big to analyze - skipping", item.Blob.Name, item.Blob.Properties.ContentLength, item.Blob.Properties.ContentLength - 4000000));
                                    Interlocked.Add(ref _imageTooBigToProcessCount, 1);
                                else
                                    try {

                                        // Make a ComputerVisionClient
                                        ComputerVisionClient client = new ComputerVisionClient(new ApiKeyServiceClientCredentials(_config.ComputerVisionKey)) { Endpoint = _config.ComputerVisionEndpoint };

                                        // Make a SAS Url of this image for ComputerVision to use for the next hour
                                        if (blobClient.CanGenerateSasUri)
                                            {
                                                // Create a SAS token that's valid for one hour.
                                                BlobSasBuilder sasBuilder = new BlobSasBuilder()
                                                {
                                                    BlobContainerName = _config.Container,
                                                    BlobName = blobClient.Name,
                                                    Resource = "b"
                                                };

                                                sasBuilder.ExpiresOn = DateTimeOffset.UtcNow.AddHours(1);
                                                sasBuilder.SetPermissions(BlobSasPermissions.Read);
                                                Uri sasUri = blobClient.GenerateSasUri(sasBuilder);

                                                // Creating a list that defines the features to be extracted from the image. 
                                                List<VisualFeatureTypes?> features = new List<VisualFeatureTypes?>()
                                                {
                                                    VisualFeatureTypes.Tags,
                                                    VisualFeatureTypes.Description,
                                                    VisualFeatureTypes.Faces
                                                };

                                                ImageAnalysis result = await client.AnalyzeImageAsync(sasUri.ToString(), visualFeatures: features);

                                                // Valid special characters: space, plus, minus, period, colon, equals, underscore, forward slash ( +-.:=_/)
                                                string description = result.Description.Captions.FirstOrDefault().Text;
                                                description = description.Replace("'", string.Empty).Replace(",", string.Empty);
                                                if(description.Length > 256)
                                                    description = description.Substring(0, 255);
                                                tags["description"] = description;

                                                string imageTags = string.Empty;
                                                result.Tags.ToList().ForEach(a => imageTags += string.Format("{0}:",a.Name));
                                                imageTags = imageTags.Trim(':');
                                                if (imageTags.Length > 256)
                                                    imageTags = imageTags.Substring(0, 255);
                                                tags["tags"] = imageTags;

                                                string faceRectangles = string.Empty;
                                                if(result.Faces.Count() > 0) {
                                                    result.Faces.ToList().ForEach(a => faceRectangles += string.Format("{0}:{1}:{2}:{3}/", a.FaceRectangle.Left, a.FaceRectangle.Top, a.FaceRectangle.Width, a.FaceRectangle.Height));
                                                    faceRectangles = faceRectangles.Trim(';');
                                                    if (faceRectangles.Length > 256)
                                                        faceRectangles = faceRectangles.Substring(0, 255);
                                                    tags["faceRectangles"] = faceRectangles;
                                                }

                                                tags["height"] = result.Metadata.Height.ToString();
                                                tags["width"] = result.Metadata.Width.ToString();

                                                if(_config.Container != "$web") {
                                                    // Create a thumbnail of this image in the static website folder. Use the ContentHash as the filename to avoid duplicates
                                                    string thumbFileName = string.Format("{0}.jpg", System.Web.HttpUtility.UrlEncode(item.Blob.Properties.ContentHash));
                                                    var thumbnailClient = webContainerClient.GetBlobClient(thumbFileName);
                                                    if (!thumbnailClient.Exists()) {
                                                        var thumbnail = await client.GenerateThumbnailAsync(256, 256, sasUri.ToString(), true);
                                                        await thumbnailClient.UploadAsync(thumbnail, new BlobUploadOptions() { HttpHeaders = new BlobHttpHeaders() { ContentType = tags["ContentType"] }, Tags = new Dictionary<string, string>() { { "description", tags["description"] } }, AccessTier = AccessTier.Hot });
                                                    }
                                                }

                                                Interlocked.Add(ref _imageProcessedCount, 1);
                                        }

                                    } catch (Exception ex) {
                                        _logger.LogError("Error Processing Blob '" + item.Blob.Name + "': " + ex.Message);
                                    } 
                            }

                            Azure.Response response = blobClient.SetTags(tags);
                            if (response.IsError) _logger.LogError(response.ReasonPhrase);

                        }

                    }

                }

                _slim.Release();

            }));

        }

    }
}
