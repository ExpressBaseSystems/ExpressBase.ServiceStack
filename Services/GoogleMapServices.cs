using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [Authenticate]
    public class GoogleMapServices : EbBaseService
    {
        public GoogleMapServices(ITenantDbFactory _dbf) : base(_dbf) { }

        public GoogleMapResponse Any(GoogleMapRequest request)
        {
            List<EbGoogleData> f = new List<EbGoogleData>();
            var _sql = "select * from eb_google_map;";
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(_sql);
            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbGoogleData
                {
                    lat = dr[1].ToString(),
                    lon = dr[2].ToString(),
                    name =  dr[3].ToString()
                });
                f.Add(_ebObject);
            }
            return new GoogleMapResponse { Data = f };
        }
    }
}



