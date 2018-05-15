using BBIN_Responder;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Dynamic;
using System.Globalization;
using System.Text;
using MongoDB.Bson;

namespace BBIN_DataResponder
{
    class Program
    {
        private static IConnection publicConnection, connection;
        private static IModel publicChannel, channel;

        static void Main(string[] args)
        {
            //mongo init
            MongoCollections collections = new MongoCollections();

            ConnectionFactory factory = new ConnectionFactory
            {
                HostName = ConnectionConstants.HostName,
                UserName = ConnectionConstants.User,
                Password = ConnectionConstants.Password,
                VirtualHost = ConnectionConstants.VHost
            };

            //factory2
            ConnectionFactory factory2 = new ConnectionFactory
            {
                HostName = ConnectionConstants.HostName,
                UserName = ConnectionConstants.User,
                Password = ConnectionConstants.Password,
                VirtualHost = ConnectionConstants.VHost2
            };

            publicConnection = factory.CreateConnection();
            publicChannel = publicConnection.CreateModel();
            publicChannel.BasicQos(0, 1, false); // Process only one message at a time

            //internal vhost
            connection = factory2.CreateConnection();
            channel = connection.CreateModel();
            string dataResponderQueue = channel.QueueDeclare().QueueName;
            channel.QueueBind(dataResponderQueue, "amq.topic", ConnectionConstants.ResponderRoutingKey);
            
            Console.WriteLine("Data Responder Ready");
            Console.WriteLine("Consume to topic {0}", ConnectionConstants.ResponderRoutingKey);
            EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(channel);
            int counter = 0;
            eventingBasicConsumer.Received += (model, ea) =>
            {
                counter++;
                String strMessage = System.Text.Encoding.UTF8.GetString(ea.Body);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(strMessage);
                Console.WriteLine("Data_Responder {1} : Received result for Task ID {0}", results.task_id, DateTime.Now);
                UpdateLog(collections, strMessage);
                PublishTask((long)results.task_id, strMessage, ConnectionConstants.QueueListener);
                // Acknowledge
                channel.BasicAck(ea.DeliveryTag, false);

            };
            channel.BasicConsume(dataResponderQueue, false, eventingBasicConsumer);

        }

        private static void UpdateLog(MongoCollections collections, string json)
        {
            JObject jObj = JObject.Parse(json);
            dynamic results = JsonConvert.DeserializeObject<dynamic>(json);

            long taskID = (long)results.task_id;
            string userID = results.user_id;
            string typeDetection = results.type;
            string typeProcc = results.results.type;
            string resultPath = results.results.resultpath;
            //JObject jObj = JObject.Parse(strMessage);
            //JObject resultObj = JObject.Parse(jObj["results"].ToString());
            BsonDocument processingResult = BsonDocument.Parse(results.results.ToString());
            JArray procRes = JArray.Parse(results.results.result.ToString());
            JArray summary = JArray.Parse(results.summary.ToString());

            BsonArray bProcRes = BsonDocument.Parse("{\"res\":" + procRes + "}")["res"].AsBsonArray;
            BsonArray deserializedArray = BsonDocument.Parse("{\"sum\":" + summary + "}")["sum"].AsBsonArray;
            //Console.WriteLine("before: " +data);
            //string processingResult = data.ToString(Formatting.None);
            //Console.WriteLine("after: " + processingResult);
            string unitProcID = results.unit_id;
            DateTime timeStart = results.time_stamp;
            TimeSpan timeProcess = DateTime.Now - timeStart;

            var filter = Builders<CommandStatusTable>.Filter.Eq(s => s.TaskId, taskID);
            var update = Builders<CommandStatusTable>.Update.Set(u => u.Status, 1)
                                                            .Set(u => u.TimeFinish, DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK"))
                                                            .Set(u => u.UnitProcessingId, unitProcID)
                                                            .Set(u => u.TimeProccess, timeProcess.ToString())
                                                            .Set("result", bProcRes)
                                                            .Set("result_path", resultPath)
                                                            .Set("summary", deserializedArray);
                            
            collections.cmd.UpdateOne(filter, update);
            Console.WriteLine("Data_Logger {1} : Write Task ID {0} to Command Status Table", taskID, DateTime.Now);
        }

        private static void PublishTask(long taskID, string message, string queueName)
        {
            byte[] messageBody = Encoding.UTF8.GetBytes(message);
            Console.WriteLine("Data_Responder {1} : Task ID {0} publish to {2}", taskID, DateTime.Now, queueName);
            publicChannel.BasicPublish("amq.topic", queueName, null, messageBody);
        }

        public static string cleanForJSON(string s)
        {
            if (s == null || s.Length == 0)
            {
                return "";
            }

            char c = '\0';
            int i;
            int len = s.Length;
            StringBuilder sb = new StringBuilder(len + 4);
            String t;

            for (i = 0; i < len; i += 1)
            {
                c = s[i];
                switch (c)
                {
                    case '\\':
                    case '"':
                        sb.Append('\\');
                        sb.Append(c);
                        break;
                    case '/':
                        sb.Append('\\');
                        sb.Append(c);
                        break;
                    case '\b':
                        sb.Append("\\b");
                        break;
                    case '\t':
                        sb.Append("\\t");
                        break;
                    case '\n':
                        sb.Append("\\n");
                        break;
                    case '\f':
                        sb.Append("\\f");
                        break;
                    case '\r':
                        sb.Append("\\r");
                        break;
                    default:
                        if (c < ' ')
                        {
                            t = "000" + String.Format("X", c);
                            sb.Append("\\u" + t.Substring(t.Length - 4));
                        }
                        else
                        {
                            sb.Append(c);
                        }
                        break;
                }
            }
            return sb.ToString();
        }
    }
}
