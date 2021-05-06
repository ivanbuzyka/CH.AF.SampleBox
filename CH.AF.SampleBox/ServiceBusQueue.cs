using System;
using CH.AF.SampleBox.Clients;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace CH.AF.SampleBox
{
    public static class ServiceBusQueue
    {
        [FunctionName("ReceiveServiceBusMessage")]
        public static async void Run([ServiceBusTrigger("test1", Connection = "Sbus1")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");

            // parsed dynamically, model type can be used if needed
            dynamic busMessage = JsonConvert.DeserializeObject(myQueueItem);            
            long assetId = busMessage?.saveEntityMessage?.TargetId;
            string assetDefinition = busMessage?.saveEntityMessage?.TargetDefinition;

            log.LogInformation($"Content Hub entity information: entityId: '{assetId}', entityDefinition: '{assetDefinition}'");

            // use client to get affected asset
            var mClient = MClientFactory.CreateClient();
            var assetEntity = await mClient.Entities.GetAsync(assetId);
            var filename = await assetEntity.GetPropertyValueAsync<string>("FileName");

            log.LogInformation($"Content hub entity information: {filename}");

            
            //perform any operations needed, e.g. connection to 3rd-parties etc.  

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
    }
}
