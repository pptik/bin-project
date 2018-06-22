using ArangoDB.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace BBIN_FaceMatchingService
{
    class persons
    {
        [DocumentProperty(Identifier = IdentifierType.Key)]
        public string _id;
        public string name;
        public string username;
        public string email;
        public string profilFacebookId;
        public JArray linkPhotosFace;
        public List<String> linkPhotos;
    }

    class Program
    {
        private static IConnection publicConnection, connection;
        private static IModel publicChannel, channel;
        private static JArray listPhotosFace = new JArray();

        static void Main(string[] args)
        {
            ArangoDatabase.ChangeSetting(s =>
            {
                s.Credential = new NetworkCredential(ConnectionConstants.ArangoUser, ConnectionConstants.ArangoPass);
            });

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

            Console.WriteLine("Face Matching SERVICE Ready");
            Console.WriteLine("Consume to topic {0}", ConnectionConstants.QueuePublisher);
            EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(publicChannel);
            int counter = 0;
            eventingBasicConsumer.Received += (model, ea) =>
            {
                counter++;
                String strMessage = System.Text.Encoding.UTF8.GetString(ea.Body);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(strMessage);
                Console.WriteLine("Service {1} : Received FB ID {0}", results.facebook_id, DateTime.Now);
                PublishTask(strMessage);
                // Acknowledge
                publicChannel.BasicAck(ea.DeliveryTag, false);

            };
            publicChannel.BasicConsume(queueTaskPublisher, false, eventingBasicConsumer);
        }

        private static void PublishTask(string message)
        {
            listPhotosFace.Clear();
            JObject jobj = new JObject();
            dynamic results = JsonConvert.DeserializeObject<dynamic>(message);
            string facebookID = results.facebook_id;
            string facebookName = results.facebook_name;
            string routingKey = "request.facetrain";
            jobj["facebook_id"] = facebookID;
            jobj["facebook_name"] = facebookName;

            using (var db = new ArangoDatabase(url: ConnectionConstants.ArangoHost, database: ConnectionConstants.ArangoDatabase))
            {
                //facebook_id example :  "100007369238236"
                var result = db.Query<persons>()
                .Where(p => p.profilFacebookId == facebookID)
                .Select(p => new persons
                {
                    _id = p._id,
                    profilFacebookId = p.profilFacebookId,
                    linkPhotosFace = p.linkPhotosFace
                }).ToList();

                var facephotos = result.First().linkPhotosFace;
                for (int i = 0; i < facephotos.Count; i++)
                {
                    var photo = facephotos[i];
                    bool isFace = (bool)facephotos[i]["verified_face"];
                    if (isFace == true)
                    {
                        listPhotosFace.Add(facephotos[i]["url"]);
                    }
                }

                jobj["list_photo_face"] = listPhotosFace;
            }

            //Console.WriteLine(jobj.ToString());
            byte[] messageBody = Encoding.UTF8.GetBytes(jobj.ToString());
            Console.WriteLine("Service {1} : FB ID {0} publish to units processing {2}, {3} face photos", facebookID, DateTime.Now, routingKey, listPhotosFace.Count);
            channel.BasicPublish("amq.topic", routingKey, null, messageBody);
        }
    }
}
