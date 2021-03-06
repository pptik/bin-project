﻿using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json.Linq;

namespace BBIN_DataLogger
{
    class CommandStatusTable : BaseModel
    {
        //taskid
        [BsonElement("task_id")]
        public long TaskId { get; set; }


        //type detection
        [BsonElement("type")]
        public string TypeDetection { get; set; }

        //pathsource
        [BsonElement("source_path")]
        public string SourcePath { get; set; }

        //status
        [BsonElement("status")]
        public int Status { get; set; }

        //timemulai
        [BsonElement("time_start")]
        public string TimeStart { get; set; }

        //timeproses
        [BsonElement("time_process")]
        public string TimeProccess { get; set; }

        //timeselesai
        [BsonElement("time_finish")]
        public string TimeFinish { get; set; }

        //idpemroses
        [BsonElement("unit_processing_id")]
        public string UnitProcessingId { get; set; }

        //userid
        [BsonElement("user_id")]
        public string UserId { get; set; }

    }
}
