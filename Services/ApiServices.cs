using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class ApiServices: EbBaseService
    {
        public ApiServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public FormDataJsonResponse Post(FormDataJsonRequest request)
        {


            return new FormDataJsonResponse { };
        }
    }
}
