using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Common.Data;
using System.Data.Common;

namespace ExpressBase.ServiceStack.Services
{
    public class SecurityServices : EbBaseService
	{
		public SecurityServices(ITenantDbFactory _dbf) : base(_dbf) { }


		public GetApplicationResponse1 Get(GetApplicationRequest1 request)
		{
			GetApplicationResponse1 resp = new GetApplicationResponse1();
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
