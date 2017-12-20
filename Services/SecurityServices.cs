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
		
		public GetManageRolesResponse Any(GetManageRolesRequest request)
		{
			string query = null;
			List<DbParameter> parameters = new List<DbParameter>();
			query = string.Format(@"SELECT id, applicationname FROM eb_applications where eb_del = FALSE ORDER BY applicationname;
									SELECT DISTINCT EO.id, EO.obj_name, EO.obj_type, EO.applicationid
										FROM eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS 
										WHERE EO.id = EOV.eb_objects_id AND EOV.id = EOS.eb_obj_ver_id AND EOS.status = 3 AND EO.applicationid > 0;
									
									SELECT id, role_name, description, applicationid FROM eb_roles ORDER BY role_name;
									SELECT id, role1_id, role2_id FROM eb_role2role WHERE eb_del = FALSE;");//if db_ok then append to 3rd query "WHERE eb_del=FALSE" 
			if (request.id > 0)
			{
				query += string.Format(@"SELECT role_name,applicationid,description FROM eb_roles WHERE id = @id;
										SELECT permissionname,obj_id,op_id FROM eb_role2permission WHERE role_id = @id AND eb_del = FALSE;
										SELECT A.applicationname, A.description FROM eb_applications A, eb_roles R WHERE A.id = R.applicationid AND R.id = @id AND A.eb_del = FALSE;

										SELECT A.id, A.firstname, B.id FROM eb_users A, eb_role2user B
											WHERE A.id = B.user_id AND A.eb_del = FALSE AND B.eb_del = FALSE AND B.role_id = @id");
				parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.id));
			}
			var ds = this.TenantDbFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
			ApplicationCollection _applicationCollection = null;
			List<Eb_RoleObject> _roleList = new List<Eb_RoleObject>();
			List<Eb_RoleToRole> _r2rList = new List<Eb_RoleToRole>();
			
			if (ds.Tables.Count > 0)
			{
				//PROCESSED RESULT
				_applicationCollection = new ApplicationCollection(ds.Tables[0], ds.Tables[1]);
				//---------------
				foreach (EbDataRow dr in ds.Tables[2].Rows)
				{
					_roleList.Add(new Eb_RoleObject() { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[2].ToString(), App_Id = Convert.ToInt32(dr[3]) });
				}
				foreach (EbDataRow dr in ds.Tables[3].Rows)
				{
					_r2rList.Add(new Eb_RoleToRole() { Id = Convert.ToInt32(dr[0]), Dominant= Convert.ToInt32(dr[1]), Dependent= Convert.ToInt32(dr[2])});
				}

			}
			Dictionary<string, object> RoleInfo = new Dictionary<string, object>();
			List<string> Permission = new List<string>();
			List<Eb_Users> _usersList = new List<Eb_Users>();

			if (ds.Tables.Count > 4)
			{
				RoleInfo.Add("RoleName", ds.Tables[4].Rows[0][0].ToString());
				RoleInfo.Add("AppId", Convert.ToInt32(ds.Tables[4].Rows[0][1]));
				RoleInfo.Add("RoleDescription", ds.Tables[4].Rows[0][2].ToString());
				RoleInfo.Add("AppName", ds.Tables[6].Rows[0][0].ToString());
				RoleInfo.Add("AppDescription", ds.Tables[6].Rows[0][1].ToString());
				foreach (var dr in ds.Tables[5].Rows)
					Permission.Add(dr[0].ToString());
				foreach (EbDataRow dr in ds.Tables[7].Rows)
				{
					_usersList.Add(new Eb_Users() { Id = Convert.ToInt32(dr[0]), Name= dr[1].ToString(), Role_Id= Convert.ToInt32(dr[2]) });
				}
			}
			return new GetManageRolesResponse() { ApplicationCollection = _applicationCollection, SelectedRoleInfo = RoleInfo, PermissionList = Permission, RoleList = _roleList, Role2RoleList = _r2rList, UsersList = _usersList };
		}



		public SaveRoleResponse Post(SaveRoleRequest request)
		{
			SaveRoleResponse resp;
			using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
			{
				con.Open();
				string sql = "SELECT eb_create_or_update_rbac_manageroles(@role_id, @applicationid, @createdby, @role_name, @description, @users, @dependants,@permission );";
				var cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
				int[] emptyarr = new int[] { };
				cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("role_id", System.Data.DbType.Int32, request.Colvalues["roleid"]));
				cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("description", System.Data.DbType.String, request.Colvalues["Description"]));
				cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("role_name", System.Data.DbType.String, request.Colvalues["role_name"]));
				cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("applicationid", System.Data.DbType.Int32, request.Colvalues["applicationid"]));
				cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("createdby", System.Data.DbType.Int32, request.UserId));
				cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("permission", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Colvalues["permission"].ToString() != string.Empty) ? request.Colvalues["permission"].ToString().Replace("[", "").Replace("]", "").Split(',').Select(n => n.ToString()).ToArray() : new string[] { }));
				cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("users", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (request.Colvalues["users"].ToString() != string.Empty) ? request.Colvalues["users"].ToString().Split(',').Select(n => Convert.ToInt32(n)).ToArray() : emptyarr));
				cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("dependants", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (request.Colvalues["dependants"].ToString() != string.Empty) ? request.Colvalues["dependants"].ToString().Replace("[", "").Replace("]", "").Split(',').Select(n => Convert.ToInt32(n)).ToArray() : emptyarr));

				resp = new SaveRoleResponse
				{
					id = Convert.ToInt32(cmd.ExecuteScalar())
				};
			}
			return resp;
		}



		//public GetObjectAndPermissionResponse Any(GetObjectAndPermissionRequest request)
		//{
		//	GetObjectAndPermissionResponse resp = new GetObjectAndPermissionResponse();
		//	string query= string.Format(@"SELECT EO.id, EO.obj_name, EO.obj_type
		//						FROM eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS 
  //                              WHERE EO.id = EOV.eb_objects_id 
		//							AND EOV.id = EOS.eb_obj_ver_id 
		//							AND EOS.status = 3 
		//							AND EO.applicationid = @applicationid;
		//					");
		//	
		//	List<DbParameter> parameters = new List<DbParameter>();
		//	if (request.RoleId > 0)
		//	{
		//		query += string.Format(@"SELECT permissionname, obj_id, op_id FROM eb_role2permission 
		//							WHERE role_id = @id AND eb_del = FALSE;
		//				");				
		//		parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.RoleId));
		//	}
			
		//	parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@applicationid", System.Data.DbType.Int32, request.AppId));
		//	var dt = this.TenantDbFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
		//	Dictionary<int, List<Eb_Object>> dlist = new Dictionary<int, List<Eb_Object>>();
		//	List<Eb_Object> objlist = new List<Eb_Object>();

		//	foreach (EbDataRow dr in dt.Tables[0].Rows)
		//	{
		//		var ob_id = Convert.ToInt32(dr[0]);
		//		var ob_name = dr[1].ToString();
		//		var ob_type = Convert.ToInt32(dr[2]);

		//		Eb_Object obj = new Eb_Object() { Obj_Id = ob_id, Obj_Name = ob_name };

		//		if (!dlist.Keys.Contains<int> (ob_type))
		//			dlist.Add(ob_type, new List<Eb_Object>());
		//		dlist[ob_type].Add(obj);
				
		//	}
		//	resp.Data = dlist;
		//	if (dt.Tables.Count > 2)
		//	{
		//		List<string> lstPermissions = new List<string>();
		//		foreach (EbDataRow dr in dt.Tables[1].Rows)
		//		{
		//			lstPermissions.Add(dr[0].ToString());
		//		}
		//		resp.Permissions = lstPermissions;
		//	}

		//	return resp;
		//}
	}	

}
