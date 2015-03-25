using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NpgsqlTests.DataModels
{
    public class CommandModel
    {
        public long JobId { get; set; }

        public int JobType { get; set; }

        public long CommandId { get; set; }

        public string CommandGuid { get; set; }

        public int Status { get; set; }

        public int Role { get; set; }

        public DateTime DueTime { get; set; }

        public DateTime StartTime { get; set; }

        public DateTime EndTime { get; set; }

        public int Duration { get; set; }

        public string Output { get; set; }

        public string OutputData { get; set; }
    }
}
