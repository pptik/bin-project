using MongoDB.Driver;

namespace BBIN_DataLogger
{
    class MongoContext
    {
        public static IMongoDatabase GetDatabase()
        {
            MongoClientSettings settings = MongoClientSettings.FromUrl(
                new MongoUrl(ConnectionConstants.MongoURI)
            );

            var client = new MongoClient(settings);
            var database = client.GetDatabase(ConnectionConstants.DatabaseName);

            return database;
        }
        
    }
}
