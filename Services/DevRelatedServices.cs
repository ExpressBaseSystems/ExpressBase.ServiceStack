using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class DevRelatedServices : EbBaseService
    {
        public DevRelatedServices(ITenantDbFactory _dbf) : base(_dbf) { }

        public GetApplicationResponse Get(GetApplicationRequest request)
        {
            GetApplicationResponse resp = new GetApplicationResponse();

            using (var con = TenantDbFactory.DataDB.GetNewConnection())
            {
                string sql = "";
                if (request.id > 0)
                {
                    sql = "SELECT * FROM eb_applications WHERE id = @id";
                    
                }
                else
                {
                    sql = "SELECT id, applicationname FROM eb_applications";
                }
                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id) };

                var dt = this.TenantDbFactory.ObjectsDB.DoQuery(sql, parameters);

                Dictionary<string, object> Dict = new Dictionary<string, object>();
                if (dt.Rows.Count > 1)
                {
                    foreach (var dr in dt.Rows)
                    {
                        Dict.Add(dr[0].ToString(), dr[1]);
                    }
                }
                else if (dt.Rows.Count == 1)
                {
                    Dict.Add("applicationname", dt.Rows[0][0]);
                    Dict.Add("description", dt.Rows[0][1]);
                }
                resp.Data = Dict;

            }
            return resp;
        }        
    }
}
