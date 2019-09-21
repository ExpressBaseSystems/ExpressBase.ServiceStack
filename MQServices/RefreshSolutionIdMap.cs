using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.Services;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    public class RefreshSolutionIdMap : EbMqBaseService
    {
        public RefreshSolutionIdMap() : base() { }

        public UpdateSidMapMqResponse Post(UpdateSidMapMqRequest request)
        {
            string q = @"SELECT esolution_id,isolution_id FROM eb_solutions WHERE eb_del=false;", esid = string.Empty, isid = string.Empty;
            try
            {
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(q);
                foreach (EbDataRow row in dt.Rows)
                {
                    esid = row["esolution_id"].ToString();
                    isid = row["isolution_id"].ToString();
                    if (string.IsNullOrEmpty(esid) || string.IsNullOrEmpty(isid))
                        continue;
                    else
                        this.Redis.Set<string>(string.Format(CoreConstants.SOLUTION_ID_MAP, esid), isid);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception at update sid map");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return new UpdateSidMapMqResponse();
        }
    }
}
