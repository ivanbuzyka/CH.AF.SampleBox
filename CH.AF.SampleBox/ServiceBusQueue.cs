using System;
using System.IO;
using System.Linq;
using System.Text;
using Azure.Storage.Blobs;
using CH.AF.SampleBox.Clients;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace CH.AF.SampleBox
{
    public static class ServiceBusQueue
    {
        private static int retryCount = 2;
        private static string blobName = "testimage.mov";

        [FunctionName("ReceiveServiceBusMessage")]
        public static async void Run([ServiceBusTrigger("test1", Connection = "Sbus1")]Message message,
                    string lockToken,
                    MessageReceiver MessageReceiver,
                    [ServiceBus("test1", Connection = "Sbus1")] MessageSender sender,
                    ILogger log)
        {
            try
            {
                log.LogInformation($"C# ServiceBus queue trigger function processed message sequence #{message.SystemProperties.SequenceNumber}");
                
                // complete message at the beginning
                await MessageReceiver.CompleteAsync(lockToken);

                string myQueueItem = Encoding.UTF8.GetString(message.Body);
                log.LogInformation($"#CHAF.SampleBox C# ServiceBus queue trigger function processed message: {myQueueItem}");
                // parsed dynamically, model type can be used if needed
                dynamic busMessage = JsonConvert.DeserializeObject(myQueueItem);
                long assetId = busMessage?.saveEntityMessage?.TargetId;
                string assetDefinition = busMessage?.saveEntityMessage?.TargetDefinition;

                log.LogInformation($"#CHAF.SampleBox Content Hub entity information: entityId: '{assetId}', entityDefinition: '{assetDefinition}'");

                // use client to get affected asset
                var mClient = MClientFactory.CreateClient();
                var assetEntity = await mClient.Entities.GetAsync(assetId);
                var filename = await assetEntity.GetPropertyValueAsync<string>("FileName");

                // testing downloadig/uploading using srtream
                var renditionName1 = "downloadOriginal";
                var rendition1 = assetEntity.GetRendition(renditionName1);
                if (rendition1 == null || rendition1.Items.Count == 0)
                {
                    log.LogError($"#CHAF.SampleBox Rendition '{renditionName1}' was not found or is empty");
                    throw new Exception($"Rendition '{renditionName1}' was not found or is empty");
                }

                log.LogInformation($"#CHAF.SampleBox Content hub entity information: {filename}");

                Stream r1 = await rendition1.Items.FirstOrDefault().GetStreamAsync();

                log.LogInformation($"#CHAF.SampleBox Streams are loaded...");

                string connectionString = Settings.StorageAccont.ConnectionString;
                BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(Settings.StorageAccont.ContainerName);
                BlobClient blobClient = containerClient.GetBlobClient($"{blobName}.zip");

                //throw new ApplicationException("artificial exception to test retry");
                await blobClient.UploadAsync(r1, true);
                throw new ApplicationException("artificial exception to test retry");
                log.LogInformation($"#CHAF.SampleBox Upload completed");
                
                //example of the service bus item (payload that received from entity create trigger)

                //     {
                //      "saveEntityMessage": {
                //          "EventType": "EntityCreated",
                //          "TimeStamp": "2021-05-06T09:17:45.776Z",
                //          "IsNew": true,
                //          "TargetDefinition": "M.Asset",
                //          "TargetId": 33670,
                //          "TargetIdentifier": "3xGOroUxi0KuCDQtcsLn_A",
                //          "CreatedOn": "2021-05-06T09:17:45.7760776Z",
                //          "UserId": 33633,
                //          "Version": 1,
                //          "ChangeSet": {
                //              "PropertyChanges": [
                //                  {
                //                      "Culture": "(Default)",
                //                      "Property": "FileName",
                //                      "Type": "System.String",
                //                      "OriginalValue": null,
                //                      "NewValue": "NHQ202008020049_medium.jpg"
                //                  },
                //                  {
                //                      "Culture": "(Default)",
                //                      "Property": "Title",
                //                      "Type": "System.String",
                //                      "OriginalValue": null,
                //                      "NewValue": "NHQ202008020049_medium.jpg"
                //                  },
                //                  {
                //                      "Culture": "(Default)",
                //                      "Property": "Asset.ExplicitApprovalRequired",
                //                      "Type": "System.Boolean",
                //                      "OriginalValue": null,
                //                      "NewValue": false
                //                  }
                //        ],
                //        "Cultures": [
                //          "(Default)"
                //        ],
                //        "RelationChanges": [
                //          {
                //          "Relation": "FinalLifeCycleStatusToAsset",
                //          "Role": 1,
                //          "Cardinality": 0,
                //          "NewValues": [
                //              542
                //          ],
                //          "RemovedValues": [],
                //          "inherits_security_original": null,
                //          "inherits_security": true
                //          },
                //            {
                //                        "Relation": "ContentRepositoryToAsset",
                //                "Role": 1,
                //                "Cardinality": 1,
                //                "NewValues": [
                //                    734
                //                ],
                //                "RemovedValues": [],
                //                "inherits_security_original": null,
                //                "inherits_security": true
                //            },
                //            {
                //                        "Relation": "DRM.Restricted.RestrictedToAsset",
                //                "Role": 1,
                //                "Cardinality": 0,
                //                "NewValues": [
                //                    886
                //                ],
                //                "RemovedValues": [],
                //                "inherits_security_original": null,
                //                "inherits_security": true
                //            }
                //        ],
                //        "inherits_security_original": null,
                //        "inherits_security": true,
                //        "is_root_taxonomy_item_original": null,
                //        "is_root_taxonomy_item": false,
                //        "is_path_root_original": null,
                //        "is_path_root": false,
                //        "is_system_owned_original": null,
                //        "is_system_owned": false
                //    }
                //            },
                //      "context": { }
                //        }
            }
            catch (Exception e)
            {
                log.LogError(e, e.Message);
                log.LogInformation("#CHAF.SampleBox Calculating exponential retry");

                // If the message doesn't have a retry-count, set as 0
                if (!message.UserProperties.ContainsKey("retry-count"))
                {
                    message.UserProperties["retry-count"] = 0;
                    message.UserProperties["original-SequenceNumber"] = message.SystemProperties.SequenceNumber;
                }

                // If there are more retries available
                if ((int)message.UserProperties["retry-count"] < retryCount)
                {
                    var retryMessage = message.Clone();
                    var retryCount = (int)message.UserProperties["retry-count"] + 1;
                    //var interval = 10 * retryCount; // Exponential interval
                    var interval = 10;
                    log.LogInformation($"Next retry in: {interval} seconds");
                    var scheduledTime = DateTimeOffset.Now.AddSeconds(interval);

                    retryMessage.UserProperties["retry-count"] = retryCount;
                    await sender.ScheduleMessageAsync(retryMessage, scheduledTime);
                                        
                    // no need to complete message here, since the message completion happens on the very beginning
                    //await MessageReceiver.CompleteAsync(lockToken);

                    log.LogInformation($"#CHAF.SampleBox Scheduling message retry {retryCount} to wait {interval} seconds and arrive at {scheduledTime.UtcDateTime}");
                }

                // If there are no more retries, deadletter the message (note the host.json config that enables this)
                else
                {
                    log.LogCritical($"#CHAF.SampleBox Exhausted all retries for message sequence # {message.UserProperties["original-SequenceNumber"]}. Message contents: {message.Body}");
                    
                    // to do - create a new message and deadletter it or send notification.
                    // await MessageReceiver.DeadLetterAsync(lockToken, "Exhausted all retries"); // this code won't work since lock will be expired
                }
            }
        }
    }
}
