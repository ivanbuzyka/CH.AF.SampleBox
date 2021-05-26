using System;
using System.Text;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace CH.AF.SampleBox
{
    public static class ServiceBusQueueWorkerScaffold
    {
        // hardcoded retry count, can be moved to the configuration
        public static int retryCount = 5;

        [FunctionName("ServiceBusQueueWorkerScaffold")]
        public static async void Run([ServiceBusTrigger("test1", Connection = "Sbus1")] Message message,
                    string lockToken,
                    MessageReceiver MessageReceiver,
                    [ServiceBus("test1", Connection = "Sbus1")] MessageSender sender,
                    ILogger log)
        {
            try
            {
                // all log messages are prefixed in order to find them in Application Insights traces easier.
                log.LogInformation($"#CHAF ServiceBus queue trigger function processed message sequence #{message.SystemProperties.SequenceNumber}");

                // complete message at the beginning. This needs to make sure lockToken won't expire during potentially long running operation
                await MessageReceiver.CompleteAsync(lockToken);

                // here comes work to do (potentially long-running operation)

                // the message body an be decoded to string
                string myQueueItem = Encoding.UTF8.GetString(message.Body);
                log.LogInformation($"#CHAF ServiceBus queue trigger function processed message: {myQueueItem}");

                // then it can be deserialized from JSON (if it is in JSON format)
                // dynamic busMessage = JsonConvert.DeserializeObject(myQueueItem);


            }
            catch (Exception e)
            {
                log.LogError(e, e.Message);
                log.LogInformation("#CHAF.SampleBox Calculating exponential retry");

                // If the message doesn't have a retry-count, set to 0. Serbice Bus message user properties are used for it
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
                    
                    // exponential interval can be calculated and used. Example below
                    //var interval = 10 * retryCount;
                    
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
                    // let's gracefully log the problem. IN case Application insights is used, it can be then simply investigated
                    // full list of failed operations can be selected using query, PowerBI, Excel etc.
                    log.LogCritical($"#CHAF Exhausted all retries for message sequence # {message.UserProperties["original-SequenceNumber"]}. Message contents: {Encoding.UTF8.GetString(message.Body)}");

                    // this code won't work since lock will be expired on long-running operation.
                    //await MessageReceiver.DeadLetterAsync(lockToken, "Exhausted all retries");
                }

                // we won't rethrow exception in order not to cause uncontrolled default retries on Azure Function
                // throw
            }
        }
    }
}
