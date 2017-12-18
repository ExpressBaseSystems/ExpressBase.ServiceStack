using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Common.Data;
using System.Data.Common;
using ExpressBase.Common;

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

		public GetManageRolesResponse Any(GetManageRolesRequest request)
		{
			string query = null;
			List<DbParameter> parameters = new List<DbParameter>();
			query = string.Format(@"SELECT id, applicationname FROM eb_applications where eb_del = FALSE;
									SELECT EO.id, EO.obj_name, EO.obj_type, EO.applicationid
										FROM eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS 
										WHERE EO.id = EOV.eb_objects_id AND EOV.id = EOS.eb_obj_ver_id AND EOS.status = 3 AND EO.applicationid > 0;");
			if (request.id > 0)
			{
				query += string.Format(@"SELECT role_name,applicationid,description FROM eb_roles WHERE id = @id;
										SELECT permissionname,obj_id,op_id FROM eb_role2permission WHERE role_id = @id AND eb_del = FALSE;
										SELECT A.applicationname, A.description FROM eb_applications A, eb_roles R WHERE A.id = R.applicationid AND R.id = @id AND A.eb_del = FALSE;");
				parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.id));
			}
			var ds = this.TenantDbFactory.ObjectsDB.DoQueries(query, parameters.ToArray());

			//PROCESSED RESULT
			ApplicationCollection _applicationCollection = new ApplicationCollection(ds.Tables[0], ds.Tables[1]);
			//---------------
			Dictionary<string, object> RoleInfo = new Dictionary<string, object>();
			List<string> Permission = new List<string>();
			if (request.id > 0)
			{
				RoleInfo.Add("RoleName", ds.Tables[2].Rows[0][0].ToString());
				RoleInfo.Add("AppId", Convert.ToInt32(ds.Tables[2].Rows[0][1]));
				RoleInfo.Add("RoleDescription", ds.Tables[2].Rows[0][2].ToString());
				RoleInfo.Add("AppName", ds.Tables[4].Rows[0][0].ToString());
				RoleInfo.Add("AppDescription", ds.Tables[4].Rows[0][1].ToString());
				foreach (var dr in ds.Tables[3].Rows)
					Permission.Add(dr[0].ToString());
			}
			return new GetManageRolesResponse() { ApplicationCollection = _applicationCollection, SelectedRoleInfo = RoleInfo, PermissionList = Permission };
		} 

		public GetObjectAndPermissionResponse Any(GetObjectAndPermissionRequest request)
		{
			GetObjectAndPermissionResponse resp = new GetObjectAndPermissionResponse();
			string query= string.Format(@" SELECT EO.id, EO.obj_name, EO.obj_type
								FROM eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS 
                                WHERE EO.id = EOV.eb_objects_id 
									AND EOV.id = EOS.eb_obj_ver_id 
									AND EOS.status = 3 
									AND EO.applicationid = @applicationid;");
			List<DbParameter> parameters = new List<DbParameter>();
			if (request.RoleId > 0)
			{
				query += string.Format(@"SELECT permissionname, obj_id, op_id FROM eb_role2permission 
							WHERE role_id = @id 
								AND eb_del = FALSE;");
				parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.RoleId));
			}
			
			parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@applicationid", System.Data.DbType.Int32, request.AppId));
			var dt = this.TenantDbFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
			Dictionary<int, List<EB_Object>> dlist = new Dictionary<int, List<EB_Object>>();
			List<EB_Object> objlist = new List<EB_Object>();

			foreach (EbDataRow dr in dt.Tables[0].Rows)
			{
				var ob_id = Convert.ToInt32(dr[0]);
				var ob_name = dr[1].ToString();
				var ob_type = Convert.ToInt32(dr[2]);

				EB_Object obj = new EB_Object() { Obj_Id = ob_id, Obj_Name = ob_name };

				if (!dlist.Keys.Contains<int> (ob_type))
					dlist.Add(ob_type, new List<EB_Object>());
				dlist[ob_type].Add(obj);
				
			}
			resp.Data = dlist;
			if (dt.Tables.Count > 2)
			{
				List<string> lstPermissions = new List<string>();
				foreach (EbDataRow dr in dt.Tables[1].Rows)
				{
					lstPermissions.Add(dr[0].ToString());
				}
				resp.Permissions = lstPermissions;
			}

			return resp;
		}
	}	

}
