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
        public CreateApplicationResponse Post(CreateApplicationRequest request)
        {
            string DbName = request.Colvalues["Isid"].ToString();            
            CreateApplicationResponse resp;
            using (var con = TenantDbFactory.DataDB.GetNewConnection(DbName.ToLower()))
            {
                con.Open();

                if (!string.IsNullOrEmpty(request.Colvalues["AppName"].ToString()))
                {
                    string sql = "";
                    if (request.Id > 0)
                    {
                         sql = "UPDATE eb_applications SET applicationname = @applicationname, description= @description WHERE id = @id RETURNING id";
                    }
                    else
                    {
                         sql = "INSERT INTO eb_applications (application_name,application_type, description,app_icon) VALUES (@applicationname,@apptype, @description,@appicon) RETURNING id";
                    }
                    var cmd = TenantDbFactory.DataDB.GetNewCommand(con, sql);
                    cmd.Parameters.Add(TenantDbFactory.ObjectsDB.GetNewParameter("applicationname", System.Data.DbType.String, request.Colvalues["AppName"]));
                    cmd.Parameters.Add(TenantDbFactory.ObjectsDB.GetNewParameter("apptype", System.Data.DbType.String, request.Colvalues["AppType"]));
                    cmd.Parameters.Add(TenantDbFactory.ObjectsDB.GetNewParameter("description", System.Data.DbType.String, request.Colvalues["Desc"]));
                    cmd.Parameters.Add(TenantDbFactory.ObjectsDB.GetNewParameter("appicon", System.Data.DbType.String, request.Colvalues["AppIcon"]));
                    var res = cmd.ExecuteScalar();
                    resp = new CreateApplicationResponse()
                    {
                        id = 1
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
