using MongoDB.Driver;
namespace BBIN_DataLogger
{
    class MongoCollections
    {
        private readonly IMongoDatabase database;

        public MongoCollections()
        {
            this.database = MongoContext.GetDatabase();
        }

        public MongoCollections(IMongoDatabase database)
        {
            this.database = database;
        }

        public IMongoCollection<CommandStatusTable> cmd => database.GetCollection<CommandStatusTable>("command_status");
    }
}
