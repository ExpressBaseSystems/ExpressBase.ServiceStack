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
                    sql = "SELECT id, applicationname FROM eb_applications WHERE id = @ id";
                    
                }
                else
                {
                    sql = "SELECT id, applicationname FROM eb_applications";
                }
                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id) };

                var dt = this.TenantDbFactory.ObjectsDB.DoQuery(sql, parameters);

                Dictionary<int, object> Dict = new Dictionary<int, object>();

                foreach (var dr in dt.Rows)
                {
                    Dict.Add(Convert.ToInt32(dr[0]), dr[1]);
                }

                resp.Data = Dict;

            }
            return resp;
        }
        public CreateApplicationResponse Post(CreateApplicationRequest request)
        {
            CreateApplicationResponse resp;
            using (var con = TenantDbFactory.DataDB.GetNewConnection())
            {
               if(!string.IsNullOrEmpty(request.Colvalues["applicationname"].ToString()))
                {
                    string sql = "INSERT INTO eb_applications (applicationname, description) VALUES (@applicationname, @description) RETURNING id";

                    DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("applicationname", System.Data.DbType.String, request.Colvalues["applicationname"]),
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("description", System.Data.DbType.String, request.Colvalues["description"])
                            };

                    var dt = this.TenantDbFactory.ObjectsDB.DoQuery(sql, parameters);

                    resp = new CreateApplicationResponse()
                    {
                        id = Convert.ToInt32(dt.Rows[0][0])
                    };
                }
               else
                {
                    resp = new CreateApplicationResponse()
                    {
                        id = 0
                    };
                }
               

            }
            return resp;
        }

    }
}
