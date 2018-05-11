using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace BBIN_DataLogger
{
    class BaseModel
    {
        public ObjectId Id { get; set; }

        public BaseModel()
        {
            Id = new ObjectId();
        }
    }
}
