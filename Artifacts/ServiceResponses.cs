using ServiceStack.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [DataContract]
    [Csv(CsvBehavior.FirstEnumerable)]
    public class EbObjectResponse
    {
        [DataMember(Order = 1)]
        public List<EbObjectWrapper> Data { get; set; }
    }
}
