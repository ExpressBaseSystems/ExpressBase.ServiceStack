using ExpressBase.UI;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [DataContract]
    [Route("/ebo", "POST")]
    public class EbObjectWrapper
    {
        [DataMember(Order = 1)]
        public int Id { get; set; }

        [DataMember(Order = 2)]
        public EbObjectType EbObjectType { get; set; }

        [DataMember(Order = 2)]
        public string Name { get; set; }

        [DataMember(Order = 4)]
        public byte[] Bytea { get; set; }

        [DataMember(Order = 5)]
        public EbObject EbObject { get; set; }

        public EbObjectWrapper() { }
        public EbObjectWrapper(int id, EbObjectType type, string name, byte[] bytea)
        {
            Id = id;
            EbObjectType = type;
            this.Name = name;
            this.Bytea = bytea;
        }
    }
}
