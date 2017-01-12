using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class EbTableColumn
    {
        public int ColId { get; set; }
        public string Name { get; set; } 
        public DbType Type { get; set; }
        public int TableId { get; set; }
    }
}
