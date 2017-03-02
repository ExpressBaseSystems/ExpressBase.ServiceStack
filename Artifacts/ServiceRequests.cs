using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [Route("/ebo")]
    [Route("/ebo/{Id}")]
    public class EbObjectRequest : IReturn<EbObjectResponse>
    {
        public int Id { get; set; }

        public string Token { get; set; }
    }
}
