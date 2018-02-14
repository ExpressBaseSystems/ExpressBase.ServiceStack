using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Common.Data;
using System.Data.Common;
using ExpressBase.Common;
using ExpressBase.Security.Core;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Structures;

namespace ExpressBase.ServiceStack.Services
{
    public class SecurityServices : EbBaseService
	{
		public SecurityServices(IEbConnectionFactory _dbf) : base(_dbf) { }

		//------COMMON LIST-------------------------------------

		public GetUsersResponse1 Any(GetUsersRequest1 request)
		{
			GetUsersResponse1 resp = new GetUsersResponse1();
			using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
			{
				con.Open();
				string sql = "SELECT id,firstname,email FROM eb_users WHERE firstname ~* @searchtext";

				DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

				var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

				List<Eb_User_ForCommonList> returndata = new List<Eb_User_ForCommonList>();
				foreach (EbDataRow dr in dt.Tables[0].Rows)
				{
					returndata.Add(new Eb_User_ForCommonList {Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Email = dr[2].ToString() });
				}
				resp.Data = returndata;
			}
			return resp;
		} //for user search

		public GetUserGroupResponse1 Any(GetUserGroupRequest1 request)
		{
			GetUserGroupResponse1 resp = new GetUserGroupResponse1();
			using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
			{
				con.Open();
				string sql = "SELECT id,name,description FROM eb_usergroup WHERE name ~* @searchtext";

				DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

				var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

				List<Eb_UserGroup_ForCommonList> returndata = new List<Eb_UserGroup_ForCommonList>();
				foreach (EbDataRow dr in dt.Tables[0].Rows)
				{
					returndata.Add(new Eb_UserGroup_ForCommonList { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[2].ToString() });
				}
				resp.Data = returndata;
			}
			return resp;
		} //for usergroup search

		public GetRolesResponse1 Any(GetRolesRequest1 request)
		{
			GetRolesResponse1 resp = new GetRolesResponse1();
			using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
			{
				con.Open();
				string sql = @"SELECT R.id,R.role_name,R.description,A.applicationname,
									(SELECT COUNT(role1_id) FROM eb_role2role WHERE role1_id=R.id AND eb_del=false) AS subrole_count,
									(SELECT COUNT(user_id) FROM eb_role2user WHERE role_id=R.id AND eb_del=false) AS user_count,
									(SELECT COUNT(distinct permissionname) FROM eb_role2permission RP, eb_objects2application OA WHERE role_id = R.id AND app_id=A.id AND RP.obj_id=OA.obj_id AND RP.eb_del = FALSE AND OA.eb_del = FALSE) AS permission_count
								FROM eb_roles R, eb_applications A
								WHERE R.applicationid = A.id AND R.role_name ~* @searchtext";
				//string sql = "SELECT R.id,R.role_name,R.description,A.applicationname FROM eb_roles R, eb_applications A WHERE R.applicationid = A.id AND R.role_name ~* @searchtext";

				DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

				var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

				List<Eb_Roles_ForCommonList> returndata = new List<Eb_Roles_ForCommonList>();
				foreach (EbDataRow dr in dt.Tables[0].Rows)
				{
					returndata.Add(new Eb_Roles_ForCommonList { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[2].ToString(),Application_Name = dr[3].ToString(), SubRole_Count = Convert.ToInt32(dr[4]), User_Count = Convert.ToInt32(dr[5]), Permission_Count = Convert.ToInt32(dr[6]) });
				}
				resp.Data = returndata;
			}
			return resp;
		} //for roles search



		//----MANAGE USER START---------------------------------
		public GetManageUserResponse Any(GetManageUserRequest request)
		{
			GetManageUserResponse resp = new GetManageUserResponse();
			string sql = @"SELECT id, role_name, description FROM eb_roles ORDER BY role_name;
                        SELECT id, name,description FROM eb_usergroup ORDER BY name;
						SELECT id, role1_id, role2_id FROM eb_role2role WHERE eb_del = FALSE;";
			if (request.Id > 0)
			{
				sql += @"SELECT fullname,nickname,email,alternateemail,dob,sex,phnoprimary,phnosecondary,landline,phextension,fbid,fbname,statusid,hide
						FROM eb_users WHERE id = @id;
						SELECT role_id FROM eb_role2user WHERE user_id = @id AND eb_del = FALSE;
						SELECT groupid FROM eb_user2usergroup WHERE userid = @id AND eb_del = FALSE;";
			}
			//SELECT firstname, email, socialid, socialname FROM eb_users WHERE id = @id;	old 4th query
			DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.Id) };
			var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

			resp.Roles = new List<EbRole>();
			foreach (var dr in ds.Tables[0].Rows)
			{
				resp.Roles.Add(new EbRole
				{
					Id = Convert.ToInt32(dr[0]),
					Name = dr[1].ToString(),
					Description = dr[2].ToString()
				});
			}

			resp.EbUserGroups = new List<EbUserGroups>();
			foreach (var dr in ds.Tables[1].Rows)
			{
				resp.EbUserGroups.Add(new EbUserGroups
				{
					Id = Convert.ToInt32(dr[0]),
					Name = dr[1].ToString(),
					Description = dr[2].ToString()

				});
			}

			resp.Role2RoleList = new List<Eb_RoleToRole>();
			foreach (EbDataRow dr in ds.Tables[2].Rows)
			{
				resp.Role2RoleList.Add(new Eb_RoleToRole() { Id = Convert.ToInt32(dr[0]), Dominant = Convert.ToInt32(dr[1]), Dependent = Convert.ToInt32(dr[2]) });
			}


			if (request.Id > 0)
			{
				resp.UserData = new Dictionary<string, string>();
				foreach (var dr in ds.Tables[3].Rows)
				{
					resp.UserData.Add("id", request.Id.ToString());
					resp.UserData.Add("fullname", dr[0].ToString());
					resp.UserData.Add("nickname", dr[1].ToString());
					resp.UserData.Add("email", dr[2].ToString());
					resp.UserData.Add("alternateemail", dr[3].ToString());
					resp.UserData.Add("dob", dr[4].ToString().Substring(0, 10));
					resp.UserData.Add("sex", dr[5].ToString());
					resp.UserData.Add("phnoprimary", dr[6].ToString());
					resp.UserData.Add("phnosecondary", dr[7].ToString());
					resp.UserData.Add("landline", dr[8].ToString());
					resp.UserData.Add("phextension", dr[9].ToString());
					resp.UserData.Add("fbid", dr[10].ToString());
					resp.UserData.Add("fbname", dr[11].ToString());
					resp.UserData.Add("statusid", dr[12].ToString());
					resp.UserData.Add("hide", dr[13].ToString());
				}

				resp.UserRoles = new List<int>();
				foreach (var dr in ds.Tables[4].Rows)
					resp.UserRoles.Add(Convert.ToInt32(dr[0]));

				resp.UserGroups = new List<int>();
				foreach (var dr in ds.Tables[5].Rows)
					resp.UserGroups.Add(Convert.ToInt32(dr[0]));
			}

			return resp;
		}

		private string GeneratePassword()
		{
			string strPwdchar = "abcdefghijklmnopqrstuvwxyz0123456789#+@&$ABCDEFGHIJKLMNOPQRSTUVWXYZ";
			string strPwd = "";
			Random rnd = new Random();
			for (int i = 0; i <= 7; i++)
			{
				int iRandom = rnd.Next(0, strPwdchar.Length - 1);
				strPwd += strPwdchar.Substring(iRandom, 1);
			}
			return strPwd;
		}

		public bool Any(UniqueCheckRequest request)
		{
			
			string sql = "SELECT id FROM eb_users WHERE email LIKE @email";
			DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("email", EbDbTypes.String, request.email) };
			var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);
			if (dt.Rows.Count > 0)
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		public SaveUserResponse Post(SaveUserRequest request)
		{
			SaveUserResponse resp;
			string sql = "";
			using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
			{
				con.Open();
				string password = "";

				if (request.Id > 0)
				{
					sql = "SELECT * FROM eb_createormodifyuserandroles(@userid,@id,@fullname,@nickname,@email,@pwd,@dob,@sex,@alternateemail,@phprimary,@phsecondary,@phlandphone,@extension,@fbid,@fbname,@roles,@group,@statusid,@hide);";

				}
				else
				{
					//password = string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) ? GeneratePassword() : (request.Colvalues["pwd"].ToString() + request.Colvalues["email"].ToString()).ToMD5Hash();
					password = GeneratePassword();
					sql = "SELECT * FROM eb_createormodifyuserandroles(@userid,@id,@fullname,@nickname,@email,@pwd,@dob,@sex,@alternateemail,@phprimary,@phsecondary,@phlandphone,@extension,@fbid,@fbname,@roles,@group,@statusid,@hide);";

				}
				int[] emptyarr = new int[] { };
				DbParameter[] parameters = 
					{
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("fullname", EbDbTypes.String, request.FullName),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("nickname", EbDbTypes.String, request.NickName),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("email", EbDbTypes.String, request.EmailPrimary),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("pwd", EbDbTypes.String,password),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("dob", EbDbTypes.Date, request.DateOfBirth),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("sex", EbDbTypes.String, request.Sex),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("alternateemail", EbDbTypes.String, request.EmailSecondary),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("phprimary", EbDbTypes.String, request.PhonePrimary),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("phsecondary", EbDbTypes.String, request.PhoneSecondary),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("phlandphone", EbDbTypes.String, request.LandPhone),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("extension", EbDbTypes.String, request.PhoneExtension),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("fbid", EbDbTypes.String, request.FbId),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("fbname", EbDbTypes.String, request.FbName),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("roles", EbDbTypes.String, (request.Roles != string.Empty? request.Roles : string.Empty)),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("group", EbDbTypes.String, (request.UserGroups != string.Empty? request.UserGroups : string.Empty)),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("statusid", EbDbTypes.Int32, Convert.ToInt32(request.StatusId)),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("hide", EbDbTypes.String, request.Hide)
					};
				
				EbDataSet dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);
				
				

				//if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) && request.Id < 0)
				//{
				//	using (var service = base.ResolveService<EmailService>())
				//	{
				//		//  service.Post(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
				//	}
				//}
				resp = new SaveUserResponse
				{
					id = Convert.ToInt32(dt.Tables[0].Rows[0][0])

				};
			}
			return resp;
		}

		//------MANAGE USER GROUP START------------------------------

		public GetManageUserGroupResponse Any(GetManageUserGroupRequest request)
		{
			List<DbParameter> parameters = new List<DbParameter>();
			List<Eb_Users> _usersList = new List<Eb_Users>();
			Dictionary<string, object> _userGroupInfo = new Dictionary<string, object>();
			if (request.id > 0)
			{
				string query = @"SELECT id,name,description FROM eb_usergroup WHERE id = @id;
							SELECT U.id,U.firstname,U.email FROM eb_users U, eb_user2usergroup G WHERE G.groupid = @id AND U.id=G.userid AND G.eb_del = FALSE;";
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.id));
				var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
				if (ds.Tables.Count > 0)
				{
					_userGroupInfo.Add("id", Convert.ToInt32(ds.Tables[0].Rows[0][0]));
					_userGroupInfo.Add("name", ds.Tables[0].Rows[0][1].ToString());
					_userGroupInfo.Add("description", ds.Tables[0].Rows[0][2].ToString());
					foreach (EbDataRow dr in ds.Tables[1].Rows)
					{
						_usersList.Add(new Eb_Users() { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Email = dr[2].ToString() });
					}
				}
			}
			else
				_userGroupInfo.Add("id", 0);

			return new GetManageUserGroupResponse() { SelectedUserGroupInfo = _userGroupInfo, UsersList = _usersList };
			//using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
			//{
			//	con.Open();
			//	string sql = "";
			//	if (request.id > 0)
			//	{
			//		sql = @"SELECT id,name,description FROM eb_usergroup WHERE id = @id;
			//                        SELECT id,firstname FROM eb_users WHERE id IN(SELECT userid FROM eb_user2usergroup WHERE groupid = @id AND eb_del = FALSE)";


			//		DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id) };

			//		var ds = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);
			//		Dictionary<string, object> result = new Dictionary<string, object>();
			//		foreach (var dr in ds.Tables[0].Rows)
			//		{

			//			result.Add("name", dr[1].ToString());
			//			result.Add("description", dr[2].ToString());
			//		}
			//		List<int> users = new List<int>();
			//		if (ds.Tables.Count > 1)
			//		{
			//			foreach (EbDataRow dr in ds.Tables[1].Rows)
			//			{
			//				users.Add(Convert.ToInt32(dr[0]));
			//				result.Add(dr[0].ToString(), dr[1]);
			//			}
			//			result.Add("userslist", users);
			//		}
			//		resp.Data = result;
			//	}
			//	else
			//	{
			//		sql = "SELECT id,name FROM eb_usergroup";
			//		var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql);

			//		Dictionary<string, object> returndata = new Dictionary<string, object>();
			//		foreach (EbDataRow dr in dt.Tables[0].Rows)
			//		{
			//			returndata[dr[0].ToString()] = dr[1].ToString();
			//		}
			//		resp.Data = returndata;
			//	}

			//}
			//return resp;
		}

		public SaveUserGroupResponse Post(SaveUserGroupRequest request)
		{
			SaveUserGroupResponse resp;
			string sql = "SELECT * FROM eb_createormodifyusergroup(@userid,@id,@name,@description,@users);";
			using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
			{
				con.Open();
				
				int[] emptyarr = new int[] { };
				DbParameter[] parameters =
					{
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("name", EbDbTypes.String, request.Name),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("description", EbDbTypes.String, request.Description),
						this.EbConnectionFactory.ObjectsDB.GetNewParameter("users", EbDbTypes.String,(request.Users != string.Empty? request.Users : string.Empty))
					};

				EbDataSet dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);



				//if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) && request.Id < 0)
				//{
				//	using (var service = base.ResolveService<EmailService>())
				//	{
				//		//  service.Post(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
				//	}
				//}
				resp = new SaveUserGroupResponse
				{
					id = Convert.ToInt32(dt.Tables[0].Rows[0][0])

				};
			}
			return resp;
		}

		//----MANAGE ROLES START---------------------------------------
		public GetManageRolesResponse Any(GetManageRolesRequest request)
		{
			string query = null;
			List<DbParameter> parameters = new List<DbParameter>();
			//old query
			//SELECT DISTINCT EO.id, EO.obj_name, EO.obj_type, EO.applicationid
			//FROM eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS
			//WHERE EO.id = EOV.eb_objects_id AND EOV.id = EOS.eb_obj_ver_id AND EOS.status = 3 AND EO.applicationid > 0;
			query = string.Format(@"SELECT id, applicationname FROM eb_applications where eb_del = FALSE ORDER BY applicationname;

									SELECT DISTINCT EO.id, EO.obj_name, EO.obj_type, EO2A.app_id
									FROM eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS, eb_objects2application EO2A 
									WHERE EO.id = EOV.eb_objects_id AND EOV.id = EOS.eb_obj_ver_id AND EOS.status = 3 
									AND EO.id = EO2A.obj_id AND EO2A.eb_del = 'false';

									SELECT id, role_name, description, applicationid, is_anonymous FROM eb_roles WHERE id <> @id ORDER BY role_name;
									SELECT id, role1_id, role2_id FROM eb_role2role WHERE eb_del = FALSE;");//if db_ok then append to 3rd query "WHERE eb_del=FALSE" 
			if (request.id > 0)
			{
				query += string.Format(@"SELECT role_name,applicationid,description,is_anonymous FROM eb_roles WHERE id = @id;
										SELECT permissionname,obj_id,op_id FROM eb_role2permission WHERE role_id = @id AND eb_del = FALSE;
										SELECT A.applicationname, A.description FROM eb_applications A, eb_roles R WHERE A.id = R.applicationid AND R.id = @id AND A.eb_del = FALSE;

										SELECT A.id, A.firstname, A.email, B.id FROM eb_users A, eb_role2user B
											WHERE A.id = B.user_id AND A.eb_del = FALSE AND B.eb_del = FALSE AND B.role_id = @id");
			}
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.id));
			var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
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
					_roleList.Add(new Eb_RoleObject() { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[2].ToString(), App_Id = Convert.ToInt32(dr[3]), Is_Anonymous = Convert.ToBoolean(dr[4]) });
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
				RoleInfo.Add("IsAnonymous", (Convert.ToBoolean(ds.Tables[4].Rows[0][3]))?"true":"false");
				RoleInfo.Add("AppName", ds.Tables[6].Rows[0][0].ToString());
				RoleInfo.Add("AppDescription", ds.Tables[6].Rows[0][1].ToString());
				foreach (var dr in ds.Tables[5].Rows)
					Permission.Add(dr[0].ToString());
				foreach (EbDataRow dr in ds.Tables[7].Rows)
				{
					_usersList.Add(new Eb_Users() { Id = Convert.ToInt32(dr[0]), Name= dr[1].ToString(),Email= dr[2].ToString(), Role2User_Id= Convert.ToInt32(dr[3]) });
				}
			}
			return new GetManageRolesResponse() { ApplicationCollection = _applicationCollection, SelectedRoleInfo = RoleInfo, PermissionList = Permission, RoleList = _roleList, Role2RoleList = _r2rList, UsersList = _usersList };
		}
		
		public GetUserDetailsResponse Any(GetUserDetailsRequest request)
		{
			string query = null;
			List<DbParameter> parameters = new List<DbParameter>();
			query = string.Format(@"SELECT id, firstname, email FROM eb_users
									WHERE LOWER(firstname) LIKE LOWER(@NAME) AND eb_del = FALSE ORDER BY firstname ASC"); 
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@NAME", EbDbTypes.String, ("%" + request.SearchText + "%")));
			var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
			List<Eb_Users> _usersList = new List<Eb_Users>();
			if (ds.Tables.Count > 0)
			{
				foreach (EbDataRow dr in ds.Tables[0].Rows)
				{
					_usersList.Add(new Eb_Users() { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Email = dr[2].ToString()});
				}
			}
			return new GetUserDetailsResponse() { UserList = _usersList };
		}
		
		public SaveRoleResponse Post(SaveRoleRequest request)
		{
			SaveRoleResponse resp;
			using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
			{
				con.Open();
				string sql = "SELECT eb_create_or_update_rbac_manageroles(@role_id, @applicationid, @createdby, @role_name, @description, @is_anonym, @users, @dependants,@permission );";
				var cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
				int[] emptyarr = new int[] { };
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("role_id", EbDbTypes.Int32, request.Colvalues["roleid"]));
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("description", EbDbTypes.String, request.Colvalues["Description"]));
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("is_anonym", EbDbTypes.Boolean, request.Colvalues["IsAnonymous"]));
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("role_name", EbDbTypes.String, request.Colvalues["role_name"]));
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("applicationid", EbDbTypes.Int32, request.Colvalues["applicationid"]));
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.Int32, request.UserId));
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("permission", EbDbTypes.String , (request.Colvalues["permission"].ToString() != string.Empty) ? request.Colvalues["permission"]: string.Empty));
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("users", EbDbTypes.String, (request.Colvalues["users"].ToString() != string.Empty) ? request.Colvalues["users"] : string.Empty));
				cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("dependants", EbDbTypes.String, (request.Colvalues["dependants"].ToString() != string.Empty) ? request.Colvalues["dependants"] : string.Empty));

				resp = new SaveRoleResponse
				{
					id = Convert.ToInt32(cmd.ExecuteScalar())
				};
			}
			return resp;
		}
		
	}	

}
