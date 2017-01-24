using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class EbModel
    {
        public int FormId { get; set; }

        public int VersionId { get; set; }

        public int TableId { get; set; }

        public bool IsEdited { get; set; }

        public Dictionary<string, object> PrimaryValues { get; set; }

        public List<EbModelLine> Lines { get; set; }

        public bool IsVoid { get; set; }

        public bool IsLocked { get; set; }

        public bool IsDeleted { get; set; }

        public EbModel()
        {
            this.PrimaryValues = new Dictionary<string, object>();
            this.Lines = new List<EbModelLine>();
        }
    }

    public class EbModelLine
    {
        public int TableId { get; set; }

        public List<Dictionary<int, object>> LineValues { get; set; }
    }
}
