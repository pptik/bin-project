using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Globalization;

namespace BBIN_DataLogger
{
    class RabbitMqService
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
            string queueTaskPublisher = publicChannel.QueueDeclare().QueueName;
            publicChannel.QueueBind(queueTaskPublisher, "amq.topic", ConnectionConstants.QueuePublisher);

            //internal vhost
            connection = factory2.CreateConnection();
            channel = connection.CreateModel();

            Console.WriteLine("Data Logger Ready");
            Console.WriteLine("Consume to topic {0}", ConnectionConstants.QueuePublisher);
            EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(publicChannel);
            int counter = 0;
            eventingBasicConsumer.Received += (model, ea) =>
            {
                counter++;
                String strMessage = System.Text.Encoding.UTF8.GetString(ea.Body);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(strMessage);
                Console.WriteLine("Data_Logger {1} : Received Task ID {0}", results.task_id, DateTime.Now);
                InsertLog(collections, strMessage);
                PublishTask((long) results.task_id, strMessage);
                // Acknowledge
                publicChannel.BasicAck(ea.DeliveryTag, false);

            };
            publicChannel.BasicConsume(queueTaskPublisher, false, eventingBasicConsumer);

            //publicChannel = null;
            //publicConnection.Close();
            //publicConnection.Dispose();
            //publicConnection = null;

            //channel = null;
            //connection.Close();
            //connection.Dispose();
            //connection = null;
        }

        private static void InsertLog(MongoCollections collections, string message)
        {
            dynamic results = JsonConvert.DeserializeObject<dynamic>(message);
            DateTime timeStart = results.time_stamp;
            CommandStatusTable doc = new CommandStatusTable()
            {
                SourcePath = results.path,
                TimeProccess = String.Empty,
                TimeFinish = String.Empty,
                UnitProcessingId = String.Empty,
                TypeDetection = results.type,
                Status = 0,
                TaskId = (long)results.task_id,
                UserId = results.user_id,
                TimeStart = timeStart.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK", CultureInfo.InvariantCulture)
        };

            collections.cmd.InsertOneAsync(doc);
            Console.WriteLine("Data_Logger {1} : Write Task ID {0} to Command Status Table", doc.TaskId, DateTime.Now);
        }

        private static void PublishTask(double taskID, string message)
        {
            dynamic results = JsonConvert.DeserializeObject<dynamic>(message);
            string type = results.type;
            byte[] messageBody = Encoding.UTF8.GetBytes(message);
            string routingKey = ConnectionConstants.QueueUnitProcessing;
            string filepath = results.path;
            string ext = System.IO.Path.GetExtension(filepath);
            switch (type) {
                case "object":
                    if (hasImageExt(ext))
                    {
                        routingKey = routingKey + ".image"; //to topic request.image
                    }
                    else {
                        routingKey = routingKey + ".video"; //to topic request.video
                    }
                    break;
                case "face":
                    routingKey = routingKey + ".face"; //to topic request.face
                    break;
            }

            Console.WriteLine("Data_Logger {1} : Task ID {0} publish to units processing {2}", taskID, DateTime.Now, routingKey);
            channel.BasicPublish("amq.topic", routingKey, null, messageBody);
        }

        private static bool hasImageExt(string ext)
        {
            if (ext == ".png" || ext == ".jpg" || ext == ".jpeg")
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private static bool WasQuitKeyPressed()
        {
            if (Console.KeyAvailable)
            {
                ConsoleKeyInfo keyInfo = Console.ReadKey();

                if (Char.ToUpperInvariant(keyInfo.KeyChar) == 'Q')
                {
                    return true;
                }
            }

            return false;
        }
    }
}
